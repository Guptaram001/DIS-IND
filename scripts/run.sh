#!/usr/bin/env bash

set -uo pipefail

# Resource configuration
#
# Every setting can be supplied before the command without editing this file:
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
# JAVA_METASPACE
#   Optional maximum class-metadata space. Example: JAVA_METASPACE=256m.
#
# JAVA_THREAD_STACK
#   Optional stack size per Java thread. Example: JAVA_THREAD_STACK=512k.
#
# GC_PARALLEL_THREADS / GC_CONCURRENT_THREADS
#   Optional G1 worker overrides. Normally leave these unset and let G1 derive
#   them from CPU_CORES.
#
# EXTRA_JAVA_OPTS
#   Additional whitespace-separated JVM flags for advanced experiments.
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

# Empty means "use the JVM/Akka default". Supplying values makes the run reproducible
CPU_CORES="${CPU_CORES:-}"
AKKA_WORKERS="${AKKA_WORKERS:-}"
JAVA_XMS="${JAVA_XMS:-}"
JAVA_XMX="${JAVA_XMX:-}"
JAVA_DIRECT_MEMORY="${JAVA_DIRECT_MEMORY:-}"
JAVA_METASPACE="${JAVA_METASPACE:-}"
JAVA_THREAD_STACK="${JAVA_THREAD_STACK:-}"
GC_PARALLEL_THREADS="${GC_PARALLEL_THREADS:-}"
GC_CONCURRENT_THREADS="${GC_CONCURRENT_THREADS:-}"

timestamp() {
    date '+%Y-%m-%dT%H:%M:%S%z'
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

require_positive_integer "CPU_CORES" "$CPU_CORES"
require_positive_integer "AKKA_WORKERS" "$AKKA_WORKERS"
require_positive_integer "GC_PARALLEL_THREADS" "$GC_PARALLEL_THREADS"
require_positive_integer "GC_CONCURRENT_THREADS" "$GC_CONCURRENT_THREADS"
require_positive_integer "SAMPLE_INTERVAL" "$SAMPLE_INTERVAL"
require_positive_integer "THREAD_DUMP_INTERVAL" "$THREAD_DUMP_INTERVAL"
require_memory_size "JAVA_XMS" "$JAVA_XMS"
require_memory_size "JAVA_XMX" "$JAVA_XMX"
require_memory_size "JAVA_DIRECT_MEMORY" "$JAVA_DIRECT_MEMORY"
require_memory_size "JAVA_METASPACE" "$JAVA_METASPACE"
require_memory_size "JAVA_THREAD_STACK" "$JAVA_THREAD_STACK"

mkdir -p "$RUN_DIR"

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
    # Intentionally allow conventional whitespace-separated JVM flags here.
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

"$JAVA_BIN" "${JVM_OPTIONS[@]}" -jar "$JAR_FILE" "$@" \
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
