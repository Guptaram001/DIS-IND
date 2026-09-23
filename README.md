# DIS-IND

Distributed, incremental, streaming-based Inclusion Dependency (IND)
discovery implemented with Akka Typed, Cluster Sharding, and Java 21.

## Run a scalable Docker cluster

Prerequisites:

- Docker Engine or Docker Desktop with Docker Compose
- At least 3 GiB of RAM available to Docker

The default dataset is configured by `UserConfig.DEFAULT_INPUT_DIR`, currently
`data/tpch-10-corrected`:

```bash
DIS_IND_EXPECTED_CLUSTER_SIZE=3 \
docker compose up --build --scale worker=2
```

The result is written to:

```text
output/ind-report.txt
```

### Run with diagnostics and resource analytics

`scripts/run.sh` builds one coordinator and any requested number of identical
worker replicas while
capturing combined logs and per-container resource samples:

```bash
DOCKER_DISTRIBUTED=1 \
WORKERS=2 \
INPUT_DIR=./data/tpch-10-corrected \
OUTPUT_DIR=./output \
SAMPLE_INTERVAL=5 \
JAVA_XMS=1g \
JAVA_XMX=4g \
./scripts/run.sh
```

Each execution creates `diagnostics/run-<timestamp>/` containing:

- `docker-compose.log`: combined coordinator and worker logs
- `container-stats.jsonl`: timestamped CPU, memory, network and block-I/O samples
- `container-status.txt`: container lifecycle snapshots
- `container-inspect.json`: final Docker configuration and exit metadata
- `final-container-status.txt`: final exit status for every node
- `ind-report.txt`: a copy of the discovery result
- `run-info.txt`: dataset, batch size, heap settings and elapsed-run metadata

`JAVA_XMX` is the maximum heap for each worker. The coordinator uses the
smaller `COORDINATOR_JAVA_XMX` because it does not host bitmap shards. Total
heap capacity is therefore `COORDINATOR_JAVA_XMX + WORKERS × JAVA_XMX`, plus
native and Docker overhead.

Application defaults are defined in `UserConfig`:

```text
input       data/tpch-10-corrected
output      output/ind-report.txt
batch size  15000
```

The same defaults apply to the single-JVM and Docker modes. Shell values
override them:

```bash
# Single JVM, UserConfig defaults
./scripts/run.sh

# Single JVM with overrides
INPUT_DIR=./data/synthetic \
OUTPUT_DIR=./output \
DIS_IND_BATCH_SIZE=400 \
./scripts/run.sh

# Distributed, UserConfig defaults
DOCKER_DISTRIBUTED=1 ./scripts/run.sh
```

Application settings are initialized once by `UserConfig`. Precedence is:
command-line option, `-Ddis.ind.*` system property, `DIS_IND_*` environment
variable, then the corresponding `UserConfig.DEFAULT_*` value.

Both local and `DOCKER_DISTRIBUTED=1` runs accept application options:

```bash
./scripts/run.sh --input-dir ./data/synthetic --batch-size 400
DOCKER_DISTRIBUTED=1 ./scripts/run.sh --batch-size 400
```

Docker input and output paths inside the containers remain `/data/input` and
`/data/output/ind-report.txt`; use `INPUT_DIR` and `OUTPUT_DIR` to select the
host directories mounted at those locations.

### Batch size and Akka frame size

The batch dispatcher sends column arrays to sharded actors over Akka remoting.
Those messages must fit inside an Artery frame. This project uses a bounded
`64 MiB` frame because a 15,000-row batch can serialize to roughly 655 KiB and
TPCH-10 final-round bitmap messages have been observed around 30.16 MiB. Both
exceed Akka's 256 KiB default. The associated buffer pool is restricted to four
buffers, bounding that reusable direct-buffer pool to roughly 256 MiB per JVM.

If the batch size is increased substantially, reduce it again or raise the
frame limit explicitly:

```bash
DOCKER_DISTRIBUTED=1 \
DIS_IND_BATCH_SIZE=30000 \
AKKA_MAXIMUM_FRAME_SIZE="64 MiB" \
AKKA_BUFFER_POOL_SIZE=4 \
./scripts/run.sh
```

Larger frames can consume more direct-buffer memory, so reducing batch size is
preferable when a very large frame would otherwise be required.

Stop and remove the containers with:

```bash
docker compose down
```

To use another directory containing `.csv` or `.tbl` files:

```bash
DIS_IND_DATA_DIR=./data/tpch \
DIS_IND_BATCH_SIZE=400 \
docker compose up --build
```

The input mount is read-only. Only the coordinator writes the final report.

## What the containers do

`coordinator` is both the stable Akka seed node and the only dataset producer.
It discovers the schema, reads the files, sends batches to the distributed
actors, waits for discovery to finish, and writes the report. It registers
sharding proxies for routing but does not host `AttributeActor` or
`CandidateManagerActor` entities.

The scalable `worker` service creates `WORKERS` replicas. Every replica joins
the coordinator's Akka cluster, registers the same shard types, and hosts
distributed `AttributeActor` and `CandidateManagerActor` entities. Workers
never read and ingest the dataset themselves.

Cluster singleton proxies ensure that logical orchestration components such as
the batch dispatcher, result collector, lattice manager, and appraisal actor
have exactly one active instance across the cluster.

All nodes mount the input directory because each must discover identical
dataset metadata before registering its shard behavior. Only the coordinator
streams the rows.

## Exact and prune calculation options

The value-based pipeline supports `count`, `witness`, `exact`, and `prune`.
Count and witness retain their existing incremental processing. Exact and prune
share one validation engine: cached LHS bitmap intersections of active attribute
clusters. The former separate Shaabani mode and `--cluster-validation scan|lhs-cache`
selector have been removed, along with the exact candidate-event flags.

```bash
./scripts/run.sh valuebased --candidate-tracking exact \
  --ind-calculation batch --cluster-change-detection true
```

Use the same flags with `--candidate-tracking prune`. Set `--ind-calculation final`
to defer all IND calculation until finalization. The environment equivalents are
`DIS_IND_IND_CALCULATION=batch|final` (default `batch`) and
`DIS_IND_CLUSTER_CHANGE_DETECTION=true|false` (default `true`). Docker and remote
launchers forward these settings; the coordinator distributes resolved cluster
options to workers.

In `batch` mode each local value-owner batch updates the clusters, recomputes
eligible LHS results, and sends only validity transitions to CMs. Final drain
waits for all acknowledged sequences, then writes the maintained result once.
There is no global synchronization/report after every input batch. Change
detection limits work to LHSs in signatures that appeared or disappeared; when
disabled, every LHS is considered and its intersection cache is invalidated at
each batch boundary. Frequency-only changes do not change cluster signatures.

Prune retains conservative LHS/RHS insertion/deletion validity skips in batch
mode. Mixed batches only skip a pair when no change on either side can reverse
its previous status. Enabled whole/partition cardinality, CQF, and transitivity
checks resolve candidates before the shared intersection. In `final` mode it
maintains summaries but performs no intermediate validity calculation or
validity-based skips. Final transitivity uses only relationships already proven
at that final boundary. Final snapshots are intersected across all buckets;
empty local LHSs impose no restriction. Both paths exclude self and incompatible
pairs and write the final file only once.

The coordinator's `INDGuardian` owns `result-collector` as a child and shares its
reference with worker guardians; the collector is not a cluster singleton.
Diagnostics record the calculation settings. `cluster-validation-metrics.tsv`
records dirty LHS counts, intersection rebuilds, signature visits, and emitted
transitions. Cluster validation and result comparison are timed under
`VALIDATION`; candidate-event timing remains for count/witness. Skip counters
now count candidate decisions at batch boundaries, not per-value event skips.

## Important environment variables

| Variable | Meaning | Default |
| --- | --- | --- |
| `DIS_IND_DATA_DIR` | Host input directory mounted into every node | `./data/tpch-10-corrected` |
| `DIS_IND_BATCH_SIZE` | Rows sent in one ingestion batch | `15000` |
| `DIS_IND_DIAGNOSTIC_EVENTS` | Log the complete DL → VO → CM event flow (very verbose) | `false` |
| `DIS_IND_OUTPUT_DIR` | Host directory receiving the report | `./output` |
| `WORKERS` | Number of worker replicas created by `run.sh` | `2` |
| `AKKA_MIN_WORKERS` | Minimum worker-role members before the cluster becomes operational; set automatically by `run.sh` | `WORKERS` |
| `JAVA_XMS` | Initial heap per container | `512m` |
| `JAVA_XMX` | Maximum heap per worker | `2g` |
| `COORDINATOR_JAVA_XMX` | Maximum coordinator heap | `2g` |
| `WORKER_CPUS` | Docker CPU quota per worker | `1.0` |
| `COORDINATOR_CPUS` | Docker CPU quota for the coordinator | `1.0` |
| `DIS_IND_VALUE_OWNER_HOT_ENTRIES` | Candidate-state cache sizing parameter per worker | `100000` |
| `DIS_IND_DRAIN_MAX_IN_FLIGHT` | Maximum unacknowledged drain batches per worker | `128` |
| `DIS_IND_DRAIN_BATCH_SIZE` | Value-owner drain records aggregated per CM message | `16` |
| `DIS_IND_DRAIN_RETRY_SECONDS` | Readiness-probe and unacknowledged-batch retry interval | `2` |
| `DIS_IND_VALUE_OWNER_DISK_DIR` | RocksDB directory for VO membership records | run diagnostics directory |
| `DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED` | Enable per-partition cardinality pruning in `prune` tracking mode | `true` |
| `DIS_IND_PRUNE_COUNT_PARTITIONS` | Number of cardinality partitions; must be a positive power of two | `64` |
| `AKKA_MAXIMUM_FRAME_SIZE` | Maximum serialized remote message frame | `64 MiB` |
| `AKKA_BUFFER_POOL_SIZE` | Reusable Artery direct buffers per node | `4` |
| `DIS_IND_NODE_ROLE` | Whether a JVM ingests (`coordinator`) or only computes (`worker`) | Set by Compose |
| `AKKA_HOSTNAME` | Address advertised to other Akka nodes | Coordinator DNS / worker replica IP |

Each distributed run writes `memory-summary.txt`, sampled container statistics,
per-node Java Flight Recorder files and per-node GC logs into its diagnostics
directory. Scaled worker files use their unique Docker container ID. The
summary identifies the node with the largest observed process memory. Open JFR
files in JDK Mission Control to inspect allocation hot spots by class and stack
trace.

`placement-summary.txt` records the final observed worker for every sharded
attribute actor (`AA`) and candidate-manager actor (`CM`). Each placement
includes the global column ID, table and local-column IDs, column and qualified
names, inferred data type and node address, followed by entity counts per
worker. During a run, watch placement changes with:

```bash
docker compose logs -f | rg --line-buffered '\[PLACEMENT\]'
```

`run.sh` sets `DIS_IND_EXPECTED_CLUSTER_SIZE` to `WORKERS + 1`, preventing the
coordinator from beginning ingestion until every requested worker has joined.
It also refuses to start when aggregate configured Java heap exceeds 75% of
Docker's available memory, leaving capacity for native memory and the Docker
VM. `ALLOW_MEMORY_OVERCOMMIT=1` bypasses this guard for deliberate experiments.

## Architecture

```text
CSV/TBL files
     |
     v
Coordinator/DataLoader (exactly one producer)
     |
     v
BatchDispatcher (cluster singleton)
     |
     +--------> AttributeActor shards across all nodes
     |                         |
     |                         v
     +--------> CandidateManagerActor shards across all nodes
                               |
                               v
                appraisal/lattice/rebuild singletons
                               |
                               v
                    ResultCollector singleton
                               |
                               v
                    output/ind-report.txt
```

Cluster sharding distributes entity state and relocates entities while the
cluster is running. It does not persist their state after all nodes stop.
Durable restart recovery would require Akka Persistence and an external journal.

## Run without Docker

Build and run a single coordinator node:

```bash
mvn -DskipTests package
DIS_IND_INPUT_DIR=./data/tpch-1 \
DIS_IND_OUTPUT_FILE=./ind-report.txt \
java -jar target/dis-ind-1.0.0.jar
```

The existing diagnostic runner is also available as `scripts/run.sh`.

## Cache ablation (value-based pipeline)

Value-ID and membership caches independently support `lru` (default) and
`caffeine` (Caffeine 3.2.4 W-TinyLFU, which combines frequency and recency).
Pass these options to `ValueBasedMain` or `scripts/run.sh`:

```bash
--value-id-cache-policy caffeine --membership-cache-policy caffeine \
--value-id-hot-entries 100000 --membership-cache-bytes 536870912
```

Equivalent environment variables are `DIS_IND_VALUE_ID_CACHE_POLICY`,
`DIS_IND_MEMBERSHIP_CACHE_POLICY`, `DIS_IND_VALUE_ID_HOT_ENTRIES`, and
`DIS_IND_MEMBERSHIP_CACHE_BYTES`. JVM properties use `dis.ind.` followed by the
CLI option name. Configure every worker consistently; Docker Compose and the
Proxmox experiment launcher forward these settings.

The value-ID limit is an entry budget per worker, divided across configured
buckets (including the remainder). Membership has a soft estimated-byte budget
per worker, also divided across buckets; zero disables retention of clean
membership entries. Dirty/in-flight membership stays pinned outside the
selectable cache, consumes that same budget, and may exceed it until existing
write backpressure catches up. Clean entries use the same estimate under both
policies: `64 + 16 * membership-column-count` bytes. Candidate-state caching,
and membership persistence retain their existing behavior.

Both policies now use the same clean/pinned ownership lifecycle. The LRU adapter
uses an access-ordered Java map, replacing the earlier fastutil hot-cache maps;
compare policies within this revision rather than attributing all differences
from older revisions solely to eviction policy. Caffeine adds boxing/metadata
and asynchronous maintenance, so logical budgets are not measured heap limits,
and instantaneous Caffeine occupancy can temporarily exceed the target.

Run the four combinations `lru/lru`, `caffeine/lru`, `lru/caffeine`, and
`caffeine/caffeine`, with fixed data order, calculation mode, seeds, batch size,
buckets, and JVM settings. Use fresh run directories and repeated runs. Repeat
at several capacities to expose the effect of memory pressure.

Worker cache TSVs include the policy, hits/misses, evictions, occupancy, and
RocksDB read/write metrics. Membership metrics additionally separate
`cache_clean_hits` from `cache_pinned_hits`, report current estimated bytes,
and report total pinned bytes (including candidate and cluster write-back state).
The shared RocksDB write durations and encoded-byte totals include cluster writes; record
counts for clusters are reported separately in `cluster-cache-metrics.tsv`. Total cache hit
rate includes pinned hits; evaluate clean-cache effectiveness using clean hits
and misses. Occupancy is approximate during concurrent maintenance. Capture JVM
heap/GC externally when comparing actual memory and runtime overhead.

Disable the value-ID hot cache with `--value-id-hot-entries 0` (or
`DIS_IND_VALUE_ID_HOT_ENTRIES=0`). No hot-cache instances are created or looked
up; metrics report policy `disabled`, zero hits, and misses for distinct values
requested within each batch. RocksDB/OS caching and within-batch deduplication
still apply. The default remains 100000 entries. To disable both clean hot
caches, also pass `--membership-cache-bytes 0`.

### Disk-backed clusters (prune and exact)

Both modes store `(bucket, signature) -> count` in `ValueOwnerMembershipStore`,
using a separate key namespace in the existing RocksDB database. They reuse the
membership writer, bounded write batches, retry handling, and backpressure.
Count and witness modes do not create cluster caches or write cluster records.

Exact and Prune also maintain `(INDEX, bucket, column, signature-bytes) -> empty`
keys in the same column family. Validation seeks the `(INDEX, bucket, lhs)` range,
decodes signatures directly from its keys, and stops at the next prefix or when
no candidates remain. Pending positive signatures are included; pending records
suppress their older disk versions, including deletions. The existing full-signature
visitor remains available for snapshots. Count and Witness neither initialize nor
use this index, and their membership/candidate paths are unchanged.

Index entries are added when a persisted signature first appears and deleted when
it disappears, atomically with the primary record in the same RocksDB WriteBatch.
The writer checks committed primary presence to avoid index rewrites for positive
count changes and to make retries idempotent. Batch byte limits conservatively
allow for index expansion. The scan metric counts index entries visited; final
cluster key bytes include secondary-index keys without increasing cluster record
counts. This uses ordered prefix ranges with the existing RocksDB filter settings;
it does not add a prefix Bloom-filter configuration.

On first open in Exact/Prune, stores without the index-version marker are backfilled
from primary signatures in bounded write batches before actors start. An interrupted
backfill can be replayed. Once indexed, keep using an index-aware binary: an older
binary would not maintain the index. Index storage repeats each signature once per
member column. The pending overlay still checks pending entries in memory; persisted
validation no longer scans unrelated signatures.

```bash
--candidate-tracking prune --cluster-cache-policy lru --cluster-cache-bytes 134217728
```

`--cluster-cache-policy` accepts `lru` (default) or `caffeine`. The separate
`--cluster-cache-bytes` budget defaults to 128 MiB per worker, divided across
configured buckets; zero disables clean cluster retention. Environment variables
are `DIS_IND_CLUSTER_CACHE_POLICY` and `DIS_IND_CLUSTER_CACHE_BYTES`; JVM properties
are `dis.ind.cluster-cache-policy` and `dis.ind.cluster-cache-bytes`. Docker Compose,
`scripts/run.sh`, and the Proxmox launcher forward these settings. Experiment and
worker-scaling YAML application settings are `cluster_cache_policy` and
`cluster_cache_bytes`.

Dirty and in-flight signature counts stay pinned until acknowledged. Their
estimated size (`128 + 8 * signature-word-count` bytes) reduces the space available
for clean entries and contributes to existing write backpressure. The budget is
soft: pending updates can exceed it, and it does not cover RocksDB/native memory,
OS page cache, result bitmaps, or pruning structures. Reads see pending updates
before disk values; zero counts delete records. Retries and older acknowledgments
do not discard newer updates. Final draining waits for pending writes in
prune/exact modes. This does not add a whole-job crash recovery mechanism.

Count lookups use the cache. Rebuilding an LHS intersection streams signatures
from disk plus pending updates without collecting all records or populating the
clean cache. This saves heap, but cache misses and intersection rebuilds add I/O,
especially with frequent deletions. Existing intersection caching and pruning rules
remain in place. Final signature reporting still materializes a list, and the
candidate manager retains distinct signatures; reporting memory is not bounded
by the cluster cache budget.

`cluster-cache-metrics.tsv` reports hits, misses, evictions, occupancy, pinned bytes,
point reads, scan records, read time, puts, and deletes. `auxiliary-storage.tsv`
reports cluster record counts and logical bytes separately. Compare runtime and
heap/GC on representative datasets before selecting the cache budget.

### Whole-column counts and phase measurements

`--prune-whole-counts-enabled false` disables the bucket-local whole-column
distinct-count rejection and allocation/maintenance of its count array.
The default is `true`. The environment variable is
`DIS_IND_PRUNE_WHOLE_COUNTS_ENABLED`; the JVM property is
`dis.ind.prune-whole-counts-enabled`. This option applies to prune batch/final
calculation, independently of partition counts, CQF, and transitive reasoning.
Previous-result reuse remains active in prune batch mode and is not configurable.

Phase TSVs now separate `membership_stage` (in-memory staging),
`rocksdb_write_execution` (membership/candidate writer execution, including
failed attempts, excluding queue wait and reply handling), `cluster_maintenance`
(cluster signatures and change tracking, in exact and prune), and `filter_update`
(auxiliary distinct/partition/CQF maintenance). `membership_update` excludes
both maintenance intervals. These are elapsed times, not CPU times; asynchronous
phases can overlap. Writer execution includes write-batch assembly and the
RocksDB write call; it does not include all RocksDB background activity.
The former `rocksdb_write` phase name is removed. Average database-read timings
are seconds per key, and average database-write timings are seconds per call
in both value-ID and membership cache metrics.

`cluster-validation-metrics.tsv` reports `affected_lhs` (formerly `dirty_lhs`)
and `derived_lhs` separately. The first counts LHSs marked affected when the
cluster-change set is consumed; the second counts actual derivation calls,
including all-LHS derivation with change detection disabled and lazy final
calculation. Re-reading a cached final result does not increase `derived_lhs`.
Both counts are cumulative per bucket, summed per worker, not unique global
columns. Final mode consumes its affected set when final derivation begins.

### Signature-based Prune metrics

`prune-metrics.tsv` includes four counters for Prune batch calculation with change
detection enabled. They count distinct candidate pairs per derived bucket/LHS row
per batch, accumulated across workers and batches, not distinct final INDs or values.
LHS rows skipped entirely by cluster change detection are not counted.

| Metric | Meaning |
|---|---|
| `signature_direct_rejections` | Candidates with a newly introduced counterexample, including already-invalid candidates. |
| `signature_preserved_results` | Candidate statuses retained without further checks: eligible pairs minus direct rejections and remaining repair checks. |
| `signature_possible_repairs` | Previously invalid candidates with a removed counterexample, before excluding new counterexamples from other values. |
| `signature_repairs_overridden` | Possible repairs excluded because the batch also introduces a counterexample for the same pair. |

Overridden repairs overlap the direct-rejection and possible-repair counters; do
not sum all four as disjoint categories. Per derived row, eligible candidates equal
direct rejections + preserved results + possible repairs - overridden repairs.
Remaining repairs proceed through the existing Prune filters and exact verification.
These counters do not change `filter_pruned_total`, which still covers whole-count,
partition-count, and CQF filtering. Legacy per-value skip counters are unchanged.
Exact, Count, Witness, final-only calculation, and change-detection-off do not
increment these new counters. Existing diagnostic files are not retroactively updated.

## Optional parallel parsing of pre-sharded input

This input path addresses the single-parser bottleneck, including the TPC-H tail
where only one large table remains. Physical shards retain the original table and
column IDs. Reader tasks follow the same round-robin batch schedule as serial
loading. Later reads may finish first but are consumed in order. Batch assembly,
delete sampling/restoration, encoding, dispatcher credits, and worker protocols
are unchanged. In this checkout batch assembly remains on the loader thread;
this feature adds parallel parsing, not a separate preparation pool.

Without a shard manifest the ordinary reader remains active. Start with two
reader threads and a shared capacity of five on a four-CPU coordinator.

Build and prepare immutable shards **on the coordinator**, outside the original
input directory (the destination must not already exist):

```bash
mvn -Dmaven.test.skip=true package
./scripts/create-csv-shards.sh /data/wikipedia /data/wikipedia-shards wikipedia 5000000
# For TPC-H, use its dataset name and input directory instead:
./scripts/create-csv-shards.sh /data/tpch /data/tpch-shards tpch-1 5000000
```

The script uses Java and the application's Commons CSV parser, so quoted commas,
embedded newlines, headers, and TPC-H trailing fields are handled as records.
Each shard contains one original batch, without a header. The manifest stores
logical table names, row offsets, counts, column counts, format, chunk size,
source sizes/mtimes, and shard SHA-256 checksums. Manifest publication happens
only after all shards are written. Failed preprocessing leaves its new directory
for inspection; rerun with a new destination. Original inputs are never modified.
Keep the original files for schema/type discovery. Do not list shards as tables.

Enable using these settings in the existing launch command:

```bash
export DIS_IND_DL_SHARD_MANIFEST=/data/wikipedia-shards/manifest.properties
export DIS_IND_DL_READER_THREADS=2
export DIS_IND_DL_READER_CAPACITY=5
# Run the existing launcher with the ORIGINAL coordinator input directory.
```

Equivalent Java CLI options are `--dl-shard-manifest`, `--dl-reader-threads`, and
`--dl-reader-capacity`; JVM properties use the `dis.ind.` prefix. For experiment
suite YAML, put `dl_shard_manifest`, `dl_reader_threads`, and `dl_reader_capacity`
under `application`. These values are forwarded by the Proxmox launcher. The
manifest and shard files must already exist on the coordinator; the launchers do
not create or transfer them. Workers do not read shards. To disable, omit/unset
the manifest setting (and remove its CLI/JVM/YAML override, if present).

For Docker via `scripts/run.sh`, create shards on the host from the same original
files mounted as input. Set `INPUT_DIR=./data/tpch-1`,
`DIS_IND_SHARD_DIR=./data/tpch-1-shards`, and
`DIS_IND_DL_SHARD_MANIFEST=/data/shards/manifest.properties`. The coordinator
mounts the host shard directory read-only at `/data/shards` and receives the
reader thread/capacity settings. `COORDINATOR_INPUT_DIR` belongs to the Proxmox
launcher, not this Docker launcher. Do not use a remote `/home/node/...` path on
your Mac. The same bind-mounted source files must pass the manifest size/mtime
checks; regenerate shards if those source fingerprints change.

Use the same dataset format and chunk size for sharding and execution. A changed
source size/mtime, format, schema, table list, or chunk size is rejected: regenerate
shards after changing/moving input files. Source fingerprints are a quick stale
input check, not a cryptographic source-content check; keep inputs immutable.
Shard checksums and row counts are verified while reading, before submission.
Account for additional disk space and report preprocessing separately when timing
repeated runs; include it when reporting one-off end-to-end cost.

**Shared capacity:** five means at most five upstream batch slots across all
readers, including queued tasks, parsing, completed rows waiting for order, and
the batch currently being assembled/submitted. It does not mean five per reader.
A slot is reserved in order before the read starts, and released only after the
existing dispatcher accepts its batch. Dispatcher queue/in-flight credits remain
separate, and delete/restore history retains its existing memory usage. This is
a batch-count bound, not a byte limit; very large records can still use substantial
memory. A capacity smaller than the reader count limits achievable concurrency.

Other logical tables retain their existing order and rounds. As smaller tables
finish, readers can work on successive shards of the remaining large table.
Insert/delete history is updated only by the ordered loader, with unchanged
seeds, batch boundaries, acknowledgement barriers, and final restoration.
Failures abort ingestion and close/cancel remaining reader tasks. Ordered parsing
can still suffer head-of-line blocking; it does not guarantee linear speedup.

Diagnostics are written under `DIS_IND_DIAGNOSTICS_DIR` (default `diagnostics`),
with an optional `dis.ind.diagnostics-dir` JVM override:

- `shard-reader-batches.tsv`: `read` events report full-shard wall/thread CPU time
  (including parsing, normalization, checksum verification and allocation).
  `consume` events report the ordered wait and approximate head-of-line wait.
  Empty timing fields are not applicable; unavailable thread CPU is `NaN`.
- `shard-reader-resources.tsv`: one-second samples of active readers, queued
  reads, occupied global slots, completed buffered tasks (including the current
  consumer-held batch), and whether an earlier unfinished read blocks a later
  completed read. Short bursts can be missed.

Head-of-line wait is sampled while the consumer waits, at up to 50 ms intervals;
its estimate may miss the beginning/end of a short blocked interval. Read wall
seconds overlap across readers and must not be summed as pipeline elapsed time.
Compare these files with `pipeline-metrics.tsv` and `batch-time-monitor.tsv` to
assess throughput and whether workers still run out of work.
