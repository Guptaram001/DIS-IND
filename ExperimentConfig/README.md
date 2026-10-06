# Cluster experiment configuration

`cluster.yaml` contains stable Proxmox VM, SSH, Git, storage, JVM and Akka
settings. `experiments/` contains algorithm and dataset settings. `suites/`
lists experiments in their execution order.

Validate a suite without contacting cluster nodes:

```bash
python3 scripts/run-experiment-suite.py \
  ExperimentConfig/cluster.yaml \
  ExperimentConfig/suites/tpch-comparison.yaml \
  --validate-only
```

Run the suite:

```bash
python3 scripts/run-experiment-suite.py \
  ExperimentConfig/cluster.yaml \
  ExperimentConfig/suites/tpch-comparison.yaml
```

The runner pulls and builds the configured Git branch once on every node. It
then runs suite entries sequentially. A run must finish and have its artifacts
collected before the next run starts. `stop_on_failure: true` stops the suite at
the first failed run; `false` cleans up the failed run and continues.

Every run gets unique coordinator output and per-node state directories. The
coordinator output contains `resolved-config.yaml`, `status.txt`,
`ind-report.txt`, and collected node logs and diagnostics.

Before launching a run, the node launcher refuses to proceed when a DIS-IND JVM
is already active, Akka port 2551 is occupied, or the generated run-state
directory already exists. This prevents an experiment from silently reusing
RocksDB or another run's process state.

Requirements on the control machine are Python 3, PyYAML, SSH and SCP. Every
cluster VM requires Git, Maven and Java 21. Configure SSH key authentication;
do not put passwords or private keys in YAML.

## Cache ablation

Add these keys under an experiment's `application` mapping:

```yaml
value_id_cache_policy: caffeine  # lru or caffeine (W-TinyLFU)
membership_cache_policy: lru     # lru or caffeine
value_id_hot_entries: 128        # MiB per worker, estimated at 128 bytes per entry; 0 disables
membership_cache_bytes: 512     # MiB per worker (legacy key name); 0 permits only pinned state
cluster_cache: lru              # lru or caffeine
cluster_cache_bytes: 128        # MiB per worker; 0 disables clean retention
```

Create four copies using `lru/lru`, `caffeine/lru`, `lru/caffeine`, and
`caffeine/caffeine`, with unique experiment names and identical remaining
settings. List them in a suite and set repeated runs. The launcher forwards
these values to both coordinator and workers; resolved configs and worker cache
TSVs identify the selected policies. Test exact and prune separately while
holding `ind_calculation` fixed. See the cache ablation section of the main
README for memory accounting and metric interpretation.

Set `application.prune_whole_counts_enabled: false` to disable whole-column
distinct-count pruning and its count-array maintenance (default: true).
Previous-result reuse remains enabled. All cluster processes receive the option.

## Worker scaling

Edit `worker-scaling.yaml`, then run `bash scripts/run-worker-scaling.sh --validate-only`
and `bash scripts/run-worker-scaling.sh`. Application settings omitted from YAML use
explicit runner defaults, independent of `DIS_IND_*` shell variables. Each
`resolved-config.yaml` records these defaults and the launcher environment.
`chunk_size` controls values per table batch (rows = max(1, chunk_size / columns));
`batch_size` is unused by this pipeline and is rejected. Reader thread/capacity
settings apply to the shard-manifest input path. Cache budgets are per worker,
so total cluster cache capacity increases with the worker count.

The launcher builds the configured remote Git branch; local uncommitted Java
changes are not deployed. Ensure the intended code is on that branch before running.

## Insert/delete benchmarks on the remote cluster

Use `worker-scaling-benchmark-insert.yaml` and
`worker-scaling-benchmark-delete.yaml`. These examples select four workers and
PRUNE; edit `scaling.worker_counts`, `scaling.modes`, repeats and remote paths
as needed. The original `worker-scaling.yaml` is unchanged.

```yaml
application:
  benchmark_operation: delete  # none (default), insert, delete
  benchmark_sample_dir: /home/node/DIS-IND/data/tpch-1-sample-v2
  ingestion_mode: insert
  ind_calculation: batch
  data_orientation: value
```

The baseline directory belongs in `dataset.input_dir`; do not use D−S for
these benchmarks. Generate the sample on the coordinator or copy the complete
sample directory there, including `manifest.json`. The sample must match the
baseline files exactly. Workers do not need the sample. Clear `dl_shard_manifest`
unless it was generated for the selected baseline.

```bash
bash scripts/run-worker-scaling.sh ExperimentConfig/worker-scaling-benchmark-delete.yaml --validate-only
bash scripts/run-worker-scaling.sh ExperimentConfig/worker-scaling-benchmark-delete.yaml
bash scripts/run-worker-scaling.sh ExperimentConfig/worker-scaling-benchmark-insert.yaml
```

Validation checks settings without contacting nodes. Actual launch checks the
sample manifest exists on the coordinator; Java validates source/sample hashes.
The coordinator receives the benchmark options, and collected coordinator
diagnostics contain `benchmark-metrics.tsv`. Experiment names include the
operation. The launcher builds the configured remote Git branch, so commit and
push the Java benchmark implementation before launching; local uncommitted Java
changes are not deployed automatically. Use identical code and settings for
both operations.
