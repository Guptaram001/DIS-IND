#!/usr/bin/env python3
"""Remove all DIS-IND worker run directories, including logs and diagnostics.

Default execution deletes deployment.state_base/runs/<suite>/<run> entirely,
plus legacy value-ids, value-to-rows and value-owners directories directly inside
state_base. Datasets outside run directories and coordinator state are preserved.
Use --dry-run to preview.
Requires local PyYAML (also used by run-experiment-suite.py), remote Python 3,
and SSH access. Stop experiments before running; do not start runs during cleanup.
"""
import argparse
from pathlib import Path
import re
import shlex
import subprocess
import sys

REMOTE_SCRIPT = r'''
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

base = Path(sys.argv[1])
dry_run = sys.argv[2] == "preview"
if not base.is_absolute() or ".." in base.parts or len(base.parts) < 4:
    raise SystemExit("Refusing unsafe state_base: " + str(base))
for part in (base, *base.parents):
    if part.is_symlink():
        raise SystemExit("Refusing symlink in state_base: " + str(part))
if not base.exists():
    print("No state directory; nothing to clean.")
    raise SystemExit(0)
if not base.is_dir():
    raise SystemExit("state_base is not a directory")

def check_idle():
    processes = subprocess.run(["ps", "-eo", "pid=,comm=,args="],
                               check=True, capture_output=True, text=True).stdout
    for line in processes.splitlines():
        fields = line.split(None, 2)
        if len(fields) == 3 and "java" in fields[1].lower() and re.search(
                r"disIND\.|dis-ind[^\s]*\.jar", fields[2], re.IGNORECASE):
            raise SystemExit("REFUSED: DIS-IND Java process is running: " + line.strip())
    # Also cover launchers that use a different Java command line.
    for parent, dirs, files in os.walk(base, followlinks=False):
        dirs[:] = [d for d in dirs if d not in ("value-ids", "value-to-rows", "value-owners")
                   and not Path(parent, d).is_symlink()]
        if "application.pid" in files:
            pid_file = Path(parent, "application.pid")
            if pid_file.is_symlink():
                raise SystemExit("Refusing symlink PID file: " + str(pid_file))
            try:
                pid = int(pid_file.read_text().strip())
                if pid <= 0:
                    raise ValueError("invalid PID")
                os.kill(pid, 0)
            except ProcessLookupError:
                continue
            except PermissionError:
                raise SystemExit("REFUSED: cannot check process in " + str(pid_file))
            except ValueError:
                raise SystemExit("REFUSED: invalid PID file " + str(pid_file))
            raise SystemExit("REFUSED: live PID in " + str(pid_file))

check_idle()
names = ("value-ids", "value-to-rows", "value-owners")
targets = []
runs = base / "runs"
if runs.is_symlink():
    raise SystemExit("Refusing symlink runs directory")
if runs.is_dir():
    for suite in sorted(runs.iterdir()):
        if suite.is_symlink():
            raise SystemExit("Refusing symlink suite: " + str(suite))
        if suite.is_dir():
            for run in sorted(suite.iterdir()):
                if run.is_symlink():
                    raise SystemExit("Refusing symlink run: " + str(run))
                if run.is_dir():
                    targets.append(run)
for name in names:
    target = base / name
    if target.is_symlink():
        raise SystemExit("Refusing symlink target: " + str(target))
    if target.exists():
        if not target.is_dir():
            raise SystemExit("Expected directory: " + str(target))
        targets.append(target)
# Preflight every target before deleting anything. Symlinks inside a run are
# unlinked by rmtree, never followed to datasets or other external directories.
def walk_error(error):
    raise error

for target in targets:
    for parent, dirs, files in os.walk(target, followlinks=False, onerror=walk_error):
        if os.path.ismount(parent):
            raise SystemExit("Refusing mounted directory: " + parent)
before = shutil.disk_usage(base).free
for target in targets:
    print(("WOULD DELETE " if dry_run else "DELETE ") + str(target), flush=True)
if not dry_run:
    check_idle()
    for target in targets:
        shutil.rmtree(target)
after = shutil.disk_usage(base).free
print("%s %d state directories; free space %.2f GiB (change %+.2f GiB)." % (
    "Previewed" if dry_run else "Deleted", len(targets), after / 2**30, (after-before) / 2**30))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).resolve().parents[1] / "ExperimentConfig/cluster.yaml")
    parser.add_argument("--dry-run", action="store_true", help="List directories without deleting them")
    args = parser.parse_args()
    try:
        import yaml
    except ImportError:
        parser.error("PyYAML is required; use the project's Python environment")
    config = yaml.safe_load(args.config.read_text())
    cluster = config["cluster"]
    user = cluster["ssh_user"]
    base = config["deployment"]["state_base"]
    hosts = list(dict.fromkeys(worker["host"] for worker in cluster["workers"]))
    if not hosts:
        parser.error("No workers configured")
    if not isinstance(user, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", user):
        parser.error("Invalid SSH user")
    if not isinstance(base, str):
        parser.error("state_base must be a string")
    for host in hosts:
        if not isinstance(host, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]*", host):
            parser.error("Invalid worker hostname")
        if host == cluster.get("coordinator", {}).get("host"):
            parser.error("Coordinator is also listed as a worker; refusing cleanup")
    remote_command = shlex.join(["python3", "-", base, "preview" if args.dry_run else "delete"])
    failed = []
    for host in hosts:
        print("\n[%s] %s" % (host, "Preview" if args.dry_run else "Cleaning worker runs and temporary databases"), flush=True)
        try:
            result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                                     user + "@" + host, remote_command], input=REMOTE_SCRIPT, text=True)
            if result.returncode:
                failed.append(host)
        except OSError as error:
            print(str(error), file=sys.stderr)
            failed.append(host)
    if failed:
        print("Cleanup failed or was refused on: " + ", ".join(failed), file=sys.stderr)
        return 1
    print("\nAll workers processed. " +
          ("No files deleted." if args.dry_run else
           "Worker run directories (including logs and diagnostics) and legacy databases deleted.") +
          " Datasets outside run directories and coordinator state were preserved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
