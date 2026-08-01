#!/usr/bin/env bash

set -uo pipefail

# Resource configuration
#
#
# Distributed Docker run with per-container analytics:
#
#   DOCKER_DISTRIBUTED=1 \
#   WORKERS=2 \
#   INPUT_DIR=./data/tpch-10-corrected \
#   OUTPUT_DIR=./output \
#   SAMPLE_INTERVAL=5 \
#   JAVA_XMS=1g \
#   JAVA_XMX=4g \
#   ./scripts/run.sh
#
# Local single-JVM run:
#
#   CPU_CORES=4 \
#   AKKA_WORKERS=4 \
#   JAVA_XMS=4g \
#   JAVA_XMX=8g \
#   JAVA_DIRECT_MEMORY=1g \
#   ./scripts/run.sh
#
# CPU_CORES
#   The number of processors the JVM behaves as if it can use. 
#
# AKKA_WORKERS
#   Fixes the Akka default dispatcher's fork join pool to this many worker threads.
#
# JAVA_XMS / JAVA_XMX
#   Initial and maximum Java object heap. XMX is not a total process RAM limit;
#
# JAVA_DIRECT_MEMORY
#   Optional maximum for NIO/direct buffers, including buffers used by Akka
#   networking. Do not set it too low. 
#
#
# GC_PARALLEL_THREADS / GC_CONCURRENT_THREADS
#   Optional G1 worker overrides. Normally leave these unset and let G1 derive
#   them from CPU_CORES.
#
#
# Suggested starting point on a machine with sufficient physical RAM:
#   CPU_CORES=4 AKKA_WORKERS=4 JAVA_XMS=4g JAVA_XMX=8g \
#   JAVA_DIRECT_MEMORY=1g ./scripts/run.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

JAVA_BIN="${JAVA_BIN:-java}"
JCMD_BIN="${JCMD_BIN:-jcmd}"
JSTAT_BIN="${JSTAT_BIN:-jstat}"
MAVEN_BIN="${MAVEN_BIN:-mvn}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-30}"
THREAD_DUMP_INTERVAL="${THREAD_DUMP_INTERVAL:-120}"
DIAGNOSTICS_BASE="${DIAGNOSTICS_BASE:-$PROJECT_DIR/diagnostics}"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="$DIAGNOSTICS_BASE/run-$RUN_ID"
JAR_FILE="$PROJECT_DIR/target/dis-ind-1.0.0.jar"
DOCKER_DISTRIBUTED="${DOCKER_DISTRIBUTED:-0}"
WORKERS="${WORKERS:-3}"
INPUT_DIR="${INPUT_DIR:-$PROJECT_DIR/data/tpch-1}"
OUTPUT_DIR="${OUTPUT_DIR:-$PROJECT_DIR/output}"
DIS_IND_BATCH_SIZE="${DIS_IND_BATCH_SIZE:-}"

# Empty means "use the JVM/Akka default". Supplying values makes the run reproducible
CPU_CORES="${CPU_CORES:-}"
AKKA_WORKERS="${AKKA_WORKERS:-}"
JAVA_XMS="${JAVA_XMS:-}"
JAVA_XMX="${JAVA_XMX:-}"
COORDINATOR_JAVA_XMX="${COORDINATOR_JAVA_XMX:-}"
JAVA_DIRECT_MEMORY="${JAVA_DIRECT_MEMORY:-}"
JAVA_METASPACE="${JAVA_METASPACE:-}"
JAVA_THREAD_STACK="${JAVA_THREAD_STACK:-}"
GC_PARALLEL_THREADS="${GC_PARALLEL_THREADS:-}"
GC_CONCURRENT_THREADS="${GC_CONCURRENT_THREADS:-}"

timestamp() {
    date '+%Y-%m-%dT%H:%M:%S%z'
}

export_docker_application_arguments() {
    while (( "$#" )); do
        local option="${1%%=*}"
        local value=""
        if [[ "$1" == *=* ]]; then
            value="${1#*=}"
            shift
        else
            if (( "$#" < 2 )); then
                echo "$option requires a value" >&2
                exit 1
            fi
            value="$2"
            shift 2
        fi
        case "$option" in
            --input-dir) export DIS_IND_INPUT_DIR="$value" ;;
            --output-file) export DIS_IND_OUTPUT_FILE="$value" ;;
            --batch-size) export DIS_IND_BATCH_SIZE="$value" ;;
            --max-tracked-violations) export DIS_IND_MAX_TRACKED_VIOLATIONS="$value" ;;
            --kmv-prune-threshold) export DIS_IND_KMV_PRUNE_THRESHOLD="$value" ;;
            --checkpoint-interval) export DIS_IND_CHECKPOINT_INTERVAL="$value" ;;
            --dl-bd-credit-window) export DIS_IND_DL_BD_CREDIT_WINDOW="$value" ;;
            --bd-aa-credit-window) export DIS_IND_BD_AA_CREDIT_WINDOW="$value" ;;
            --batch-ack-timeout-seconds) export DIS_IND_BATCH_ACK_TIMEOUT_SECONDS="$value" ;;
            --final-cm-drain-timeout-seconds) export DIS_IND_FINAL_CM_DRAIN_TIMEOUT_SECONDS="$value" ;;
            --store-value-strings) export DIS_IND_STORE_VALUE_STRINGS="$value" ;;
            --value-id-hot-entries) export DIS_IND_VALUE_ID_HOT_ENTRIES="$value" ;;
            --value-id-disk-dir) export DIS_IND_VALUE_ID_DISK_DIR="$value" ;;
            --value-to-rows-disk-dir) export DIS_IND_VALUE_TO_ROWS_DISK_DIR="$value" ;;
            --checkpoint-writers-per-node) export DIS_IND_CHECKPOINT_WRITERS_PER_NODE="$value" ;;
            *)
                echo "Unknown application option: $option" >&2
                exit 1
                ;;
        esac
    done
}

require_positive_integer() {
    local setting_name="$1"
    local setting_value="$2"
    if [[ -n "$setting_value" && ! "$setting_value" =~ ^[1-9][0-9]*$ ]]; then
        echo "$setting_name must be a positive integer; received: $setting_value" >&2
        exit 1
    fi
}

require_memory_size() {
    local setting_name="$1"
    local setting_value="$2"
    if [[ -n "$setting_value" && ! "$setting_value" =~ ^[1-9][0-9]*[kKmMgGtT]?$ ]]; then
        echo "$setting_name must look like 512m, 4g or 8192m; received: $setting_value" >&2
        exit 1
    fi
}

memory_size_to_bytes() {
    local value="$1"
    local number="$value"
    local suffix=""
    local multiplier=1
    case "$value" in
        *[kKmMgGtT])
            suffix="${value#${value%?}}"
            number="${value%?}"
            ;;
    esac
    case "$suffix" in
        k|K) multiplier=1024 ;;
        m|M) multiplier=$((1024 * 1024)) ;;
        g|G) multiplier=$((1024 * 1024 * 1024)) ;;
        t|T) multiplier=$((1024 * 1024 * 1024 * 1024)) ;;
    esac
    echo $((number * multiplier))
}

require_positive_integer "CPU_CORES" "$CPU_CORES"
require_positive_integer "AKKA_WORKERS" "$AKKA_WORKERS"
require_positive_integer "GC_PARALLEL_THREADS" "$GC_PARALLEL_THREADS"
require_positive_integer "GC_CONCURRENT_THREADS" "$GC_CONCURRENT_THREADS"
require_positive_integer "SAMPLE_INTERVAL" "$SAMPLE_INTERVAL"
require_positive_integer "THREAD_DUMP_INTERVAL" "$THREAD_DUMP_INTERVAL"
require_positive_integer "WORKERS" "$WORKERS"
require_memory_size "JAVA_XMS" "$JAVA_XMS"
require_memory_size "JAVA_XMX" "$JAVA_XMX"
require_memory_size "COORDINATOR_JAVA_XMX" "$COORDINATOR_JAVA_XMX"
require_memory_size "JAVA_DIRECT_MEMORY" "$JAVA_DIRECT_MEMORY"
require_memory_size "JAVA_METASPACE" "$JAVA_METASPACE"
require_memory_size "JAVA_THREAD_STACK" "$JAVA_THREAD_STACK"

mkdir -p "$RUN_DIR"

run_distributed_docker() {
    export_docker_application_arguments "$@"
    if ! command -v docker >/dev/null 2>&1; then
        echo "Required command not found: docker" >&2
        exit 1
    fi
    if [[ ! -d "$INPUT_DIR" ]]; then
        echo "Input directory not found: $INPUT_DIR" >&2
        exit 1
    fi

    local input_dir_abs
    local output_dir_abs
    input_dir_abs="$(cd "$INPUT_DIR" && pwd)"
    mkdir -p "$OUTPUT_DIR"
    output_dir_abs="$(cd "$OUTPUT_DIR" && pwd)"

    export DIS_IND_DATA_DIR="$input_dir_abs"
    export DIS_IND_OUTPUT_DIR="$output_dir_abs"
    export DIS_IND_DIAGNOSTICS_DIR="$RUN_DIR"
    export DIS_IND_EXPECTED_CLUSTER_SIZE=$((WORKERS + 1))
    if [[ -n "$DIS_IND_BATCH_SIZE" ]]; then
        export DIS_IND_BATCH_SIZE
    else
        unset DIS_IND_BATCH_SIZE
    fi
   
    export JAVA_XMS="${JAVA_XMS:-512m}"
    export JAVA_XMX="${JAVA_XMX:-2g}"
    export COORDINATOR_JAVA_XMX="${COORDINATOR_JAVA_XMX:-2g}"

    local docker_memory_bytes
    local aggregate_heap_bytes
    docker_memory_bytes="$(docker info --format '{{.MemTotal}}')"
    aggregate_heap_bytes=$((
        $(memory_size_to_bytes "$COORDINATOR_JAVA_XMX") +
        $(memory_size_to_bytes "$JAVA_XMX") * WORKERS
    ))
    if (( aggregate_heap_bytes * 100 > docker_memory_bytes * 75 )); then
        echo "ERROR: aggregate JVM Xmx is more than 75% of Docker memory." >&2
        echo "  Docker memory: $docker_memory_bytes bytes" >&2
        echo "  JVM heap capacity: $aggregate_heap_bytes bytes (1 coordinator + $WORKERS workers)" >&2
        echo "Leave room for Akka direct buffers, metaspace, JFR, Docker, and the VM." >&2
        echo "Increase Docker memory or reduce WORKERS/JAVA_XMX." >&2
        echo "Set ALLOW_MEMORY_OVERCOMMIT=1 only if you intentionally accept OOM risk." >&2
        if [[ "${ALLOW_MEMORY_OVERCOMMIT:-0}" != "1" ]]; then
            exit 1
        fi
    fi

    if [[ -f "$output_dir_abs/ind-report.txt" ]]; then
        mv "$output_dir_abs/ind-report.txt" "$RUN_DIR/preexisting-ind-report.txt"
    fi

    {
        echo "run_id=$RUN_ID"
        echo "mode=docker-distributed"
        echo "project_dir=$PROJECT_DIR"
        echo "started_at=$(timestamp)"
        echo "input_dir=$input_dir_abs"
        echo "output_dir=$output_dir_abs"
        echo "batch_size=${DIS_IND_BATCH_SIZE:-UserConfig default}"
        echo "workers=$WORKERS"
        echo "expected_cluster_size=$DIS_IND_EXPECTED_CLUSTER_SIZE"
        echo "sample_interval_seconds=$SAMPLE_INTERVAL"
        echo "java_xms_per_container=$JAVA_XMS"
        echo "coordinator_java_xmx=$COORDINATOR_JAVA_XMX"
        echo "worker_java_xmx=$JAVA_XMX"
        echo "aggregate_java_xmx_bytes=$aggregate_heap_bytes"
        docker info --format 'docker_memory_bytes={{.MemTotal}} docker_cpus={{.NCPU}}'
        docker version
        docker compose version
    } > "$RUN_DIR/run-info.txt" 2>&1

    echo "Starting the DIS-IND Docker cluster"
    echo "Topology: 1 coordinator + $WORKERS worker replica(s)"
    echo "Input dataset: $input_dir_abs"
    echo "Diagnostics directory: $RUN_DIR"

    (
        cd "$PROJECT_DIR" &&
        docker compose up --build --force-recreate --remove-orphans --scale "worker=$WORKERS" \
            --abort-on-container-exit --exit-code-from coordinator
    ) > >(tee "$RUN_DIR/docker-compose.log") 2>&1 &
    APP_PID=$!
    echo "$APP_PID" > "$RUN_DIR/docker-compose.pid"

    monitor_docker_application() {
        local sample_number=0
        while kill -0 "$APP_PID" 2>/dev/null; do
            sample_number=$((sample_number + 1))
            {
                echo "===== $(timestamp) sample=$sample_number ====="
                cd "$PROJECT_DIR" && docker compose ps -a
            } >> "$RUN_DIR/container-status.txt" 2>&1 || true

            local container_ids
            container_ids="$(cd "$PROJECT_DIR" && docker compose ps -aq)"
            if [[ -n "$container_ids" ]]; then
                {
                    echo "===== $(timestamp) sample=$sample_number ====="
                    # One JSON object per container: CPU, RAM, network and block I/O.
                    docker stats --no-stream --format '{{json .}}' $container_ids
                } >> "$RUN_DIR/container-stats.jsonl" 2>&1 || true
                {
                    
                    docker stats --no-stream \
                        --format '{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}\t{{.PIDs}}' \
                        $container_ids
                } >> "$RUN_DIR/container-stats.tsv" 2>&1 || true
            fi
            sleep "$SAMPLE_INTERVAL"
        done
    }

    monitor_docker_application &
    MONITOR_PID=$!

    wait "$APP_PID"
    APP_STATUS=$?
    kill "$MONITOR_PID" 2>/dev/null || true
    wait "$MONITOR_PID" 2>/dev/null || true

    {
        cd "$PROJECT_DIR" || exit
        docker compose ps -a
        echo
        docker compose images
    } > "$RUN_DIR/final-container-status.txt" 2>&1

    {
        cd "$PROJECT_DIR" || exit
        container_ids="$(docker compose ps -aq)"
        if [[ -n "$container_ids" ]]; then
            docker inspect $container_ids
        else
            echo "[]"
        fi
    } > "$RUN_DIR/container-inspect.json" 2>&1

    if [[ -s "$RUN_DIR/container-stats.tsv" ]]; then
        awk -F '\t' '
            NF >= 5 {
                pct = $3
                gsub(/%/, "", pct)
                if (!( $1 in peak) || pct + 0 > peak[$1] + 0) {
                    peak[$1] = pct
                    usage[$1] = $2
                    cpu[$1] = $4
                    pids[$1] = $5
                }
            }
            END {
                print "Peak sampled container memory"
                print "container\tmemory_at_peak\tpercent_of_docker_vm\tcpu_at_sample\tpids"
                winner = ""
                for (name in peak) {
                    print name "\t" usage[name] "\t" peak[name] "%\t" cpu[name] "\t" pids[name]
                    if (winner == "" || peak[name] + 0 > peak[winner] + 0)
                        winner = name
                }
                if (winner != "")
                    print "\nmax_memory_container=" winner
            }
        ' "$RUN_DIR/container-stats.tsv" > "$RUN_DIR/memory-summary.txt"
    fi

    if [[ -s "$RUN_DIR/docker-compose.log" ]]; then
        awk '
            /\[PLACEMENT\]/ {
                separator = index($0, "|")
                if (separator == 0)
                    next
                container = substr($0, 1, separator - 1)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", container)
                type = col = tableId = table = localCol = columnName = qualifiedName = dataType = node = ""
                for (i = 1; i <= NF; i++) {
                    if ($i ~ /^type=/) { type = $i; sub(/^type=/, "", type) }
                    if ($i ~ /^col=/)  { col = $i;  sub(/^col=/, "", col) }
                    if ($i ~ /^tableId=/) { tableId = $i; sub(/^tableId=/, "", tableId) }
                    if ($i ~ /^table=/) { table = $i; sub(/^table=/, "", table) }
                    if ($i ~ /^localCol=/) { localCol = $i; sub(/^localCol=/, "", localCol) }
                    if ($i ~ /^columnName=/) { columnName = $i; sub(/^columnName=/, "", columnName) }
                    if ($i ~ /^qualifiedName=/) { qualifiedName = $i; sub(/^qualifiedName=/, "", qualifiedName) }
                    if ($i ~ /^dataType=/) { dataType = $i; sub(/^dataType=/, "", dataType) }
                    if ($i ~ /^node=/) { node = $i; sub(/^node=/, "", node) }
                }
                if (type != "" && col != "") {
                    key = type ":" col
                    owner[key] = container
                    address[key] = node
                    entityType[key] = type
                    entityCol[key] = col
                    entityTableId[key] = tableId
                    entityTable[key] = table
                    entityLocalCol[key] = localCol
                    entityColumnName[key] = columnName
                    entityQualifiedName[key] = qualifiedName
                    entityDataType[key] = dataType
                }
            }
            END {
                print "Final observed sharded-entity placement"
                print "container\ttype\tglobal_column\ttable_id\ttable\tlocal_column\tcolumn_name\tqualified_name\tdata_type\tnode"
                for (key in owner) {
                    print owner[key] "\t" entityType[key] "\t" entityCol[key] "\t" \
                        entityTableId[key] "\t" entityTable[key] "\t" entityLocalCol[key] "\t" \
                        entityColumnName[key] "\t" entityQualifiedName[key] "\t" \
                        entityDataType[key] "\t" address[key]
                    count[owner[key] SUBSEP entityType[key]]++
                }
                print "\nEntity counts by container"
                print "container\ttype\tcount"
                for (key in count) {
                    split(key, parts, SUBSEP)
                    print parts[1] "\t" parts[2] "\t" count[key]
                }
            }
        ' "$RUN_DIR/docker-compose.log" > "$RUN_DIR/placement-summary.txt"
    fi

    if [[ -f "$output_dir_abs/ind-report.txt" ]]; then
        cp "$output_dir_abs/ind-report.txt" "$RUN_DIR/ind-report.txt"
    fi

    {
        echo "finished_at=$(timestamp)"
        echo "exit_status=$APP_STATUS"
    } >> "$RUN_DIR/run-info.txt"

    echo "Distributed run exited with status $APP_STATUS"
    echo "Diagnostics saved in: $RUN_DIR"
    if [[ -f "$RUN_DIR/memory-summary.txt" ]]; then
        echo
        cat "$RUN_DIR/memory-summary.txt"
    fi
    if [[ -f "$RUN_DIR/placement-summary.txt" ]]; then
        echo
        sed -n '/Entity counts by container/,$p' "$RUN_DIR/placement-summary.txt"
    fi
    exit "$APP_STATUS"
}

if [[ "$DOCKER_DISTRIBUTED" == "1" ]]; then
    run_distributed_docker "$@"
fi


mkdir -p "$OUTPUT_DIR"
export DIS_IND_INPUT_DIR="${DIS_IND_INPUT_DIR:-$INPUT_DIR}"
export DIS_IND_OUTPUT_FILE="${DIS_IND_OUTPUT_FILE:-$OUTPUT_DIR/ind-report.txt}"
if [[ -n "$DIS_IND_BATCH_SIZE" ]]; then
    export DIS_IND_BATCH_SIZE
fi

for required_command in "$JAVA_BIN" "$JCMD_BIN" "$JSTAT_BIN"; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        echo "Required command not found: $required_command" >&2
        exit 1
    fi
done

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
    if ! command -v "$MAVEN_BIN" >/dev/null 2>&1; then
        echo "Required command not found: $MAVEN_BIN" >&2
        exit 1
    fi
    echo "Building the shaded application JAR..."
    (cd "$PROJECT_DIR" && "$MAVEN_BIN" -DskipTests package) \
        > >(tee "$RUN_DIR/maven-build.log") 2>&1
fi

if [[ ! -f "$JAR_FILE" ]]; then
    echo "Executable JAR not found: $JAR_FILE" >&2
    echo "Run without SKIP_BUILD=1, or verify the Maven artifact name." >&2
    exit 1
fi

JVM_OPTIONS=(
    "-XX:+HeapDumpOnOutOfMemoryError"
    "-XX:HeapDumpPath=$RUN_DIR"
    "-XX:ErrorFile=$RUN_DIR/hs_err_pid%p.log"
    "-XX:NativeMemoryTracking=summary"
    "-Xlog:gc*,safepoint:file=$RUN_DIR/gc.log:time,uptime,level,tags:filecount=5,filesize=100M"
    "-XX:StartFlightRecording=name=disIND,settings=profile,filename=$RUN_DIR/disIND.jfr,disk=true,dumponexit=true,maxsize=2g"
)

if [[ -n "$CPU_CORES" ]]; then
    JVM_OPTIONS+=("-XX:ActiveProcessorCount=$CPU_CORES")
fi

if [[ -n "$AKKA_WORKERS" ]]; then
   
    JVM_OPTIONS+=(
        "-Dakka.actor.default-dispatcher.fork-join-executor.parallelism-min=$AKKA_WORKERS"
        "-Dakka.actor.default-dispatcher.fork-join-executor.parallelism-factor=1.0"
        "-Dakka.actor.default-dispatcher.fork-join-executor.parallelism-max=$AKKA_WORKERS"
    )
fi

if [[ -n "$JAVA_XMS" ]]; then
    JVM_OPTIONS+=("-Xms$JAVA_XMS")
fi

if [[ -n "$JAVA_XMX" ]]; then
    JVM_OPTIONS+=("-Xmx$JAVA_XMX")
fi

if [[ -n "$JAVA_DIRECT_MEMORY" ]]; then
    JVM_OPTIONS+=("-XX:MaxDirectMemorySize=$JAVA_DIRECT_MEMORY")
fi

if [[ -n "$JAVA_METASPACE" ]]; then
    JVM_OPTIONS+=("-XX:MaxMetaspaceSize=$JAVA_METASPACE")
fi

if [[ -n "$JAVA_THREAD_STACK" ]]; then
    JVM_OPTIONS+=("-Xss$JAVA_THREAD_STACK")
fi

if [[ -n "$GC_PARALLEL_THREADS" ]]; then
    JVM_OPTIONS+=("-XX:ParallelGCThreads=$GC_PARALLEL_THREADS")
fi

if [[ -n "$GC_CONCURRENT_THREADS" ]]; then
    JVM_OPTIONS+=("-XX:ConcGCThreads=$GC_CONCURRENT_THREADS")
fi

if [[ -n "${EXTRA_JAVA_OPTS:-}" ]]; then
    read -r -a EXTRA_OPTIONS <<< "$EXTRA_JAVA_OPTS"
    JVM_OPTIONS+=("${EXTRA_OPTIONS[@]}")
fi

{
    echo "run_id=$RUN_ID"
    echo "project_dir=$PROJECT_DIR"
    echo "jar=$JAR_FILE"
    echo "started_at=$(timestamp)"
    echo "sample_interval_seconds=$SAMPLE_INTERVAL"
    echo "thread_dump_interval_seconds=$THREAD_DUMP_INTERVAL"
    echo "cpu_cores=${CPU_CORES:-JVM default}"
    echo "akka_workers=${AKKA_WORKERS:-Akka default}"
    echo "java_xms=${JAVA_XMS:-JVM default}"
    echo "java_xmx=${JAVA_XMX:-JVM default}"
    echo "java_direct_memory=${JAVA_DIRECT_MEMORY:-JVM default}"
    echo "java_metaspace=${JAVA_METASPACE:-JVM default}"
    echo "java_thread_stack=${JAVA_THREAD_STACK:-JVM default}"
    echo "gc_parallel_threads=${GC_PARALLEL_THREADS:-G1 automatic}"
    echo "gc_concurrent_threads=${GC_CONCURRENT_THREADS:-G1 automatic}"
    echo "extra_java_opts=${EXTRA_JAVA_OPTS:-none}"
    uname -a
    "$JAVA_BIN" -version
} > "$RUN_DIR/run-info.txt" 2>&1

APP_PID=""
MONITOR_PID=""

stop_processes() {
    local signal="${1:-TERM}"
    if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
        kill "$MONITOR_PID" 2>/dev/null || true
    fi
    if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" 2>/dev/null; then
        kill -"$signal" "$APP_PID" 2>/dev/null || true
    fi
}

trap 'stop_processes TERM' INT TERM

echo "Starting disIND.DisINDMain"
echo "Diagnostics directory: $RUN_DIR"

(
    cd "$PROJECT_DIR" || exit 1
    exec "$JAVA_BIN" "${JVM_OPTIONS[@]}" -jar "$JAR_FILE" "$@"
) \
    > >(tee "$RUN_DIR/application-console.log") 2>&1 &
APP_PID=$!
echo "$APP_PID" > "$RUN_DIR/application.pid"

collect_static_details() {
    "$JCMD_BIN" "$APP_PID" VM.version > "$RUN_DIR/vm-version.txt" 2>&1 || true
    "$JCMD_BIN" "$APP_PID" VM.command_line > "$RUN_DIR/vm-command-line.txt" 2>&1 || true
    "$JCMD_BIN" "$APP_PID" VM.flags > "$RUN_DIR/vm-flags.txt" 2>&1 || true
    "$JCMD_BIN" "$APP_PID" VM.system_properties > "$RUN_DIR/vm-properties.txt" 2>&1 || true
}

monitor_application() {
    local sample_number=0
    local elapsed=0

    while kill -0 "$APP_PID" 2>/dev/null; do
        sample_number=$((sample_number + 1))
        {
            echo "===== $(timestamp) sample=$sample_number ====="
            "$JSTAT_BIN" -gcutil "$APP_PID" 1000 1
        } >> "$RUN_DIR/gc-util.txt" 2>&1 || true

        {
            echo "===== $(timestamp) sample=$sample_number ====="
            "$JCMD_BIN" "$APP_PID" GC.heap_info
        } >> "$RUN_DIR/heap-samples.txt" 2>&1 || true

        {
            echo "===== $(timestamp) sample=$sample_number ====="
            "$JCMD_BIN" "$APP_PID" VM.native_memory summary
        } >> "$RUN_DIR/native-memory-samples.txt" 2>&1 || true

        if command -v top >/dev/null 2>&1; then
            {
                echo "===== $(timestamp) sample=$sample_number ====="
                top -l 1 -pid "$APP_PID" -stats pid,cpu,mem,vsize,threads,time
            } >> "$RUN_DIR/os-process-samples.txt" 2>&1 || true
        fi

        if command -v vm_stat >/dev/null 2>&1; then
            {
                echo "===== $(timestamp) sample=$sample_number ====="
                vm_stat
            } >> "$RUN_DIR/os-memory-samples.txt" 2>&1 || true
        fi

        if (( elapsed % THREAD_DUMP_INTERVAL == 0 )); then
            "$JCMD_BIN" "$APP_PID" Thread.print -l \
                > "$RUN_DIR/thread-dump-$sample_number.txt" 2>&1 || true
        fi

        sleep "$SAMPLE_INTERVAL"
        elapsed=$((elapsed + SAMPLE_INTERVAL))
    done
}

sleep 2
if kill -0 "$APP_PID" 2>/dev/null; then
    collect_static_details
    monitor_application &
    MONITOR_PID=$!
fi

wait "$APP_PID"
APP_STATUS=$?

if [[ -n "$MONITOR_PID" ]]; then
    kill "$MONITOR_PID" 2>/dev/null || true
    wait "$MONITOR_PID" 2>/dev/null || true
fi

{
    echo "finished_at=$(timestamp)"
    echo "exit_status=$APP_STATUS"
} >> "$RUN_DIR/run-info.txt"

echo "Application exited with status $APP_STATUS"
echo "Diagnostics saved in: $RUN_DIR"
exit "$APP_STATUS"
