#!/usr/bin/env python3
"""Run value-based DIS-IND experiments sequentially on a Proxmox VM cluster."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile

try:
    import yaml
except ImportError as exc:
    raise SystemExit("PyYAML is required: python3 -m pip install PyYAML") from exc


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "scripts" / "run-proxmox-cluster.sh"
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return value


def require(mapping: dict, key: str, context: str):
    if key not in mapping:
        raise ValueError(f"Missing {context}.{key}")
    return mapping[key]


def checked_name(value: object, context: str) -> str:
    name = str(value)
    if not SAFE_NAME.fullmatch(name):
        raise ValueError(f"{context} must contain only letters, digits, '.', '_' and '-': {name}")
    return name


def bool_text(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).lower()
    if text not in {"true", "false"}:
        raise ValueError(f"Expected boolean value, received {value!r}")
    return text


def run_launcher(action: str, environment: dict[str, str], check: bool = True) -> int:
    completed = subprocess.run([str(LAUNCHER), action], env={**os.environ, **environment}, check=False)
    if check and completed.returncode != 0:
        raise subprocess.CalledProcessError(completed.returncode, [str(LAUNCHER), action])
    return completed.returncode


def ssh_target(environment: dict[str, str]) -> str:
    return f"{environment['SSH_USER']}@{environment['COORDINATOR']}"


def remote_write(environment: dict[str, str], path: str, contents: str) -> None:
    parent = str(Path(path).parent)
    command = f"mkdir -p {shlex.quote(parent)} && printf %s {shlex.quote(contents)} > {shlex.quote(path)}"
    subprocess.run(["ssh", ssh_target(environment), command], check=True)


def remote_commit(environment: dict[str, str]) -> str:
    completed = subprocess.run(
        ["ssh", ssh_target(environment), "git", "-C", environment["REMOTE_PROJECT_DIR"], "rev-parse", "HEAD"],
        check=True,
        text=True,
        capture_output=True,
    )
    return completed.stdout.strip()


def copy_resolved_config(environment: dict[str, str], destination: str, resolved: dict) -> None:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as stream:
        yaml.safe_dump(resolved, stream, sort_keys=False)
        temporary = Path(stream.name)
    try:
        subprocess.run(
            ["scp", str(temporary), f"{ssh_target(environment)}:{destination}/resolved-config.yaml"],
            check=True,
        )
    finally:
        temporary.unlink(missing_ok=True)


def base_environment(cluster_config: dict) -> tuple[dict[str, str], dict]:
    cluster = require(cluster_config, "cluster", "root")
    deployment = require(cluster_config, "deployment", "root")
    coordinator = require(cluster, "coordinator", "cluster")
    workers = require(cluster, "workers", "cluster")
    if not isinstance(workers, list) or not workers:
        raise ValueError("cluster.workers must be a non-empty list")

    worker_hosts = [str(require(worker, "host", "cluster.workers[]")) for worker in workers]
    worker_ips = [str(worker.get("advertised_ip", worker["host"])) for worker in workers]
    coordinator_host = str(require(coordinator, "host", "cluster.coordinator"))
    coordinator_ip = str(coordinator.get("advertised_ip", coordinator_host))
    worker_defaults = cluster.get("worker_defaults", {})
    akka = cluster.get("akka", {})

    environment = {
        "SSH_USER": str(require(cluster, "ssh_user", "cluster")),
        "COORDINATOR": coordinator_host,
        "COORDINATOR_IP": coordinator_ip,
        "WORKERS": " ".join(worker_hosts),
        "WORKER_IPS": " ".join(worker_ips),
        "GIT_URL": str(require(deployment, "git_url", "deployment")),
        "GIT_BRANCH": str(require(deployment, "git_branch", "deployment")),
        "REMOTE_PROJECT_DIR": str(require(deployment, "project_dir", "deployment")),
        "REMOTE_STATE_BASE": str(require(deployment, "state_base", "deployment")),
        "COORDINATOR_OUTPUT_BASE": str(require(coordinator, "output_base", "cluster.coordinator")),
        "JAVA_XMS": str(worker_defaults.get("java_xms", "2g")),
        "WORKER_JAVA_XMX": str(worker_defaults.get("java_xmx", "8g")),
        "COORDINATOR_JAVA_XMX": str(coordinator.get("java_xmx", "4g")),
        "AKKA_PORT": str(akka.get("port", 2551)),
        "AKKA_MAXIMUM_FRAME_SIZE": str(akka.get("maximum_frame_size", "64 MiB")),
        "AKKA_BUFFER_POOL_SIZE": str(akka.get("buffer_pool_size", 4)),
        "CLUSTER_START_TIMEOUT_SECONDS": str(akka.get("startup_timeout_seconds", 600)),
    }
    return environment, coordinator


APPLICATION_VARIABLES = {
    "batch_size": "DIS_IND_BATCH_SIZE",
    "chunk_size": "DIS_IND_CHUNK_SIZE",
    "data_orientation": "DIS_IND_DATA_ORIENTATION",
    "candidate_tracking": "DIS_IND_CANDIDATE_TRACKING",
    "ingestion_mode": "DIS_IND_INGESTION_MODE",
    "prune_cqf_enabled": "DIS_IND_PRUNE_CQF_ENABLED",
    "prune_partition_counts_enabled": "DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED",
    "prune_partition_hierarchy_enabled": "DIS_IND_PRUNE_PARTITION_HIERARCHY_ENABLED",
    "prune_transitive_enabled": "DIS_IND_PRUNE_TRANSITIVE_ENABLED",
    "prune_count_partitions": "DIS_IND_PRUNE_COUNT_PARTITIONS",
    "cluster_validation": "DIS_IND_CLUSTER_VALIDATION",
    "exact_event_filtering_enabled": "DIS_IND_EXACT_EVENT_FILTERING_ENABLED",
    "exact_direct_violation_enabled": "DIS_IND_EXACT_DIRECT_VIOLATION_ENABLED",
}


BOOLEAN_APPLICATION_SETTINGS = {
    "prune_cqf_enabled",
    "prune_partition_counts_enabled",
    "prune_partition_hierarchy_enabled",
    "prune_transitive_enabled",
    "exact_event_filtering_enabled",
    "exact_direct_violation_enabled",
}


def experiment_environment(base: dict[str, str], coordinator: dict, experiment: dict,
                           suite_name: str, run_name: str) -> tuple[dict[str, str], str]:
    application = experiment.get("application", {})
    dataset = require(experiment, "dataset", "experiment")
    resources = experiment.get("resources", {})
    output_dir = f"{base['COORDINATOR_OUTPUT_BASE']}/{suite_name}/{run_name}"
    state_dir = f"{base['REMOTE_STATE_BASE']}/runs/{suite_name}/{run_name}"

    environment = dict(base)
    environment.update({
        "COORDINATOR_INPUT_DIR": str(require(dataset, "input_dir", "dataset")),
        "COORDINATOR_OUTPUT_DIR": output_dir,
        "REMOTE_STATE_DIR": state_dir,
        "REMOTE_LOG_DIR": f"{state_dir}/logs",
        "DIS_IND_DATASET_NAME": str(require(dataset, "name", "dataset")),
        "EXPERIMENT_TIMEOUT_SECONDS": str(experiment.get("experiment", {}).get("timeout_seconds", 21600)),
        "COORDINATOR_JAVA_XMX": str(resources.get(
            "coordinator_java_xmx", coordinator.get("java_xmx", base["COORDINATOR_JAVA_XMX"]))),
        "WORKER_JAVA_XMX": str(resources.get("worker_java_xmx", base["WORKER_JAVA_XMX"])),
    })
    for yaml_key, environment_key in APPLICATION_VARIABLES.items():
        if yaml_key in application:
            value = bool_text(application[yaml_key]) if yaml_key in BOOLEAN_APPLICATION_SETTINGS else str(application[yaml_key])
            environment[environment_key] = value
    return environment, output_dir


def resolve_experiment_path(reference: str, suite_path: Path, config_root: Path) -> Path:
    candidate = (suite_path.parent / reference).resolve()
    if candidate.is_file():
        return candidate
    candidate = (config_root / reference).resolve()
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(f"Experiment file not found: {reference}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cluster_config", type=Path)
    parser.add_argument("suite_config", type=Path)
    parser.add_argument("--skip-prepare", action="store_true", help="Do not pull and build before the suite")
    parser.add_argument("--validate-only", action="store_true", help="Validate YAML without contacting nodes")
    args = parser.parse_args()

    cluster_path = args.cluster_config.resolve()
    suite_path = args.suite_config.resolve()
    cluster_config = load_yaml(cluster_path)
    suite_config = load_yaml(suite_path)
    suite = require(suite_config, "suite", "root")
    suite_name = checked_name(require(suite, "name", "suite"), "suite.name")
    references = require(suite, "experiments", "suite")
    if not isinstance(references, list) or not references:
        raise ValueError("suite.experiments must be a non-empty list")

    base, coordinator = base_environment(cluster_config)
    if args.validate_only:
        for reference in references:
            experiment_path = resolve_experiment_path(str(reference), suite_path, cluster_path.parent)
            experiment = load_yaml(experiment_path)
            metadata = require(experiment, "experiment", "experiment root")
            experiment_name = checked_name(require(metadata, "name", "experiment"), "experiment.name")
            repeats = int(metadata.get("repeats", 1))
            if repeats <= 0:
                raise ValueError(f"experiment.repeats must be positive in {experiment_path}")
            experiment_environment(base, coordinator, experiment, suite_name, f"{experiment_name}-validation")
            print(f"Valid: {experiment_path} (enabled={bool(metadata.get('enabled', True))}, repeats={repeats})")
        print(f"Suite '{suite_name}' is valid")
        return 0

    if not args.skip_prepare:
        run_launcher("prepare", base)
    commit = remote_commit(base)
    stop_on_failure = bool(suite.get("stop_on_failure", True))
    failures = 0

    for reference in references:
        experiment_path = resolve_experiment_path(str(reference), suite_path, cluster_path.parent)
        experiment = load_yaml(experiment_path)
        metadata = require(experiment, "experiment", "experiment root")
        if not bool(metadata.get("enabled", True)):
            print(f"Skipping disabled experiment {experiment_path}", flush=True)
            continue
        experiment_name = checked_name(require(metadata, "name", "experiment"), "experiment.name")
        repeats = int(metadata.get("repeats", 1))
        if repeats <= 0:
            raise ValueError(f"experiment.repeats must be positive in {experiment_path}")

        for repetition in range(1, repeats + 1):
            timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
            run_name = checked_name(f"{experiment_name}-run-{repetition:03d}-{timestamp}", "run name")
            environment, output_dir = experiment_environment(
                base, coordinator, experiment, suite_name, run_name)
            resolved = {
                "suite": suite_name,
                "run": run_name,
                "git_commit": commit,
                "cluster": cluster_config,
                "experiment": experiment,
            }
            print(f"\n=== Starting {suite_name}/{run_name} ===", flush=True)
            remote_write(environment, f"{output_dir}/status.txt", "RUNNING\n")
            copy_resolved_config(environment, output_dir, resolved)

            status = "SUCCESS"
            try:
                run_launcher("launch", environment)
                run_launcher("wait", environment)
            except subprocess.CalledProcessError as exc:
                status = f"FAILED exit={exc.returncode}"
                failures += 1
                run_launcher("stop", environment, check=False)
            finally:
                collect_code = run_launcher("collect", environment, check=False)
                if collect_code != 0 and status == "SUCCESS":
                    status = f"FAILED collection_exit={collect_code}"
                    failures += 1
                remote_write(environment, f"{output_dir}/status.txt", status + "\n")

            print(f"=== Finished {suite_name}/{run_name}: {status} ===", flush=True)
            if status != "SUCCESS" and stop_on_failure:
                return 1

    return 1 if failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
