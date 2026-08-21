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
| `DIS_IND_VALUE_OWNER_HOT_ENTRIES` | Decoded VO membership records cached per worker | `100000` |
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
