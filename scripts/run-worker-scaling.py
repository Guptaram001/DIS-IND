#!/usr/bin/env python3
"""Expand one scaling configuration into temporary inputs for the existing suite runner."""
import argparse
import copy
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile

SCRIPT_DIR = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('experiment_suite', SCRIPT_DIR / 'run-experiment-suite.py')
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)
yaml = suite.yaml


def build_plan(path):
    config = suite.load_yaml(path)
    unknown = set(config) - {'cluster_config', 'scaling', 'dataset', 'application', 'resources'}
    if unknown:
        raise ValueError(f'Unknown settings: {sorted(unknown)}')
    inventory = suite.load_yaml((path.parent / config['cluster_config']).resolve())
    scaling = config['scaling']
    unknown = set(scaling) - {'worker_counts', 'modes', 'repeats', 'timeout_seconds', 'stop_on_failure', 'excluded_workers'}
    if unknown:
        raise ValueError(f'Unknown scaling settings: {sorted(unknown)}')
    excluded = set(scaling.get('excluded_workers', []))
    workers = [w for w in inventory['cluster']['workers']
               if w['host'] not in excluded and w.get('advertised_ip', w['host']) not in excluded]
    if not workers:
        raise ValueError('No workers remain after exclusions')
    hosts = [w['host'] for w in workers]
    if len(set(hosts)) != len(hosts):
        raise ValueError('Worker inventory contains duplicate hosts')
    counts = scaling.get('worker_counts', 'all')
    if counts == 'all':
        counts = list(range(1, len(workers) + 1))
    if not isinstance(counts, list) or not counts or any(type(n) is not int or n < 1 or n > len(workers) for n in counts):
        raise ValueError(f'worker_counts must be all or a list between 1 and {len(workers)}')
    if len(set(counts)) != len(counts):
        raise ValueError('worker_counts must not contain duplicates')
    modes = scaling.get('modes', ['prune', 'exact'])
    if not isinstance(modes, list) or not modes or any(m not in ['prune', 'exact', 'count', 'witness'] for m in modes):
        raise ValueError('modes must be a non-empty list of prune, exact, count, or witness')
    if len(set(modes)) != len(modes):
        raise ValueError('modes must not contain duplicates')
    for key in ['repeats', 'timeout_seconds']:
        value = scaling.get(key, 1 if key == 'repeats' else 3600)
        if type(value) is not int or value < 1:
            raise ValueError(f'{key} must be a positive integer')
    if type(scaling.get('stop_on_failure', True)) is not bool:
        raise ValueError('stop_on_failure must be boolean')
    application = config['application']
    unknown = set(application) - set(suite.APPLICATION_VARIABLES)
    if unknown:
        raise ValueError(f'Unsupported application settings: {sorted(unknown)}')
    if 'candidate_tracking' in application:
        raise ValueError('Use scaling.modes instead of application.candidate_tracking')
    for key, choices in {'ind_calculation': ['final', 'batch'], 'ingestion_mode': ['insert', 'delete'],
                         'value_id_cache_policy': ['lru', 'caffeine'], 'membership_cache_policy': ['lru', 'caffeine'],
                         'cluster_cache': ['lru', 'caffeine']}.items():
        if key in application and application[key] not in choices:
            raise ValueError(f'{key} must be one of {choices}')
    dataset = config['dataset']
    name = suite.checked_name(dataset['name'], 'dataset.name')
    if not str(dataset['input_dir']).strip():
        raise ValueError('dataset.input_dir must not be empty')
    plans = []
    for count in counts:
        cluster = copy.deepcopy(inventory)
        cluster['cluster']['workers'] = workers[:count]
        suite_name = f'{name}-{count}-workers'
        experiments = []
        base, coordinator = suite.base_environment(cluster)
        for mode in modes:
            experiment = {'experiment': {'name': f'{name}-{mode}-{application.get("ind_calculation", "batch")}',
                          'enabled': True, 'repeats': scaling.get('repeats', 1),
                          'timeout_seconds': scaling.get('timeout_seconds', 3600)},
                          'dataset': dataset, 'application': {**application, 'candidate_tracking': mode},
                          'resources': config.get('resources', {})}
            suite.experiment_environment(base, coordinator, experiment, suite_name, 'validation')
            experiments.append(experiment)
        plans.append((cluster, suite_name, experiments))
    return plans, scaling.get('stop_on_failure', True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('--validate-only', action='store_true')
    parser.add_argument('--skip-prepare', action='store_true')
    args = parser.parse_args()
    plans, stop_on_failure = build_plan(args.config.resolve())
    for cluster, name, experiments in plans:
        print(f'{name}: workers={[w["host"] for w in cluster["cluster"]["workers"]]}, '
              f'modes={[e["application"]["candidate_tracking"] for e in experiments]}', flush=True)
    if args.validate_only:
        print('Configuration valid; no nodes contacted.')
        return 0
    failed = False
    with tempfile.TemporaryDirectory(prefix='dis-ind-scaling-') as temp:
        root = Path(temp)
        for cluster, name, experiments in plans:
            cluster_path = root / 'cluster.yaml'
            cluster_path.write_text(yaml.safe_dump(cluster, sort_keys=False))
            references = []
            for index, experiment in enumerate(experiments):
                p = root / f'experiment-{index}.yaml'
                p.write_text(yaml.safe_dump(experiment, sort_keys=False))
                references.append(str(p))
            suite_path = root / 'suite.yaml'
            suite_path.write_text(yaml.safe_dump({'suite': {'name': name, 'stop_on_failure': stop_on_failure,
                                                          'experiments': references}}, sort_keys=False))
            command = [sys.executable, str(SCRIPT_DIR / 'run-experiment-suite.py'), str(cluster_path), str(suite_path)]
            if args.skip_prepare:
                command.append('--skip-prepare')
            result = subprocess.run(command)
            if result.returncode:
                failed = True
                if stop_on_failure:
                    return result.returncode
    return int(failed)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (KeyError, ValueError, TypeError, OSError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
