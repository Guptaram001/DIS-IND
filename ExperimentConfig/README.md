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
value_id_hot_entries: 100000     # per-worker total; 0 disables the value-ID hot cache
membership_cache_bytes: 536870912  # per-worker estimated budget; 0 permits only pinned state
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
