#!/usr/bin/env bash

set -euo pipefail

# Bare-metal/VM launcher for one coordinator and seven workers. Override any
# setting as an environment variable instead of editing the launch logic.
SSH_USER="${SSH_USER:-$USER}"
COORDINATOR="${COORDINATOR:-10.10.10.101}"
COORDINATOR_IP="${COORDINATOR_IP:-$COORDINATOR}"
WORKERS_TEXT="${WORKERS:-10.10.10.102 10.10.10.103 10.10.10.104 10.10.10.105 10.10.10.106 10.10.10.107 10.10.10.108}"
read -r -a WORKER_HOSTS <<< "$WORKERS_TEXT"
WORKER_IPS_TEXT="${WORKER_IPS:-$WORKERS_TEXT}"
read -r -a WORKER_ADDRESSES <<< "$WORKER_IPS_TEXT"
if [[ "${#WORKER_HOSTS[@]}" -ne "${#WORKER_ADDRESSES[@]}" ]]; then
    echo "WORKERS and WORKER_IPS must contain the same number of entries" >&2
    exit 2
fi

GIT_URL="${GIT_URL:-git@github.com:Guptaram001/DIS-IND.git}"
GIT_BRANCH="${GIT_BRANCH:-pruneTest}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/home/$SSH_USER/dis-ind}"
COORDINATOR_INPUT_DIR="${COORDINATOR_INPUT_DIR:-/data/dis-ind/input}"
COORDINATOR_OUTPUT_DIR="${COORDINATOR_OUTPUT_DIR:-/data/dis-ind/output}"
REMOTE_STATE_DIR="${REMOTE_STATE_DIR:-/home/$SSH_USER/.local/state/dis-ind}"
REMOTE_LOG_DIR="${REMOTE_LOG_DIR:-/home/$SSH_USER/.local/state/dis-ind/logs}"

JAVA_XMS="${JAVA_XMS:-2g}"
WORKER_JAVA_XMX="${WORKER_JAVA_XMX:-8g}"
COORDINATOR_JAVA_XMX="${COORDINATOR_JAVA_XMX:-4g}"
CLUSTER_START_TIMEOUT_SECONDS="${CLUSTER_START_TIMEOUT_SECONDS:-600}"
AKKA_PORT="${AKKA_PORT:-2551}"
AKKA_MAXIMUM_FRAME_SIZE="${AKKA_MAXIMUM_FRAME_SIZE:-64MiB}"
AKKA_MAXIMUM_FRAME_SIZE="${AKKA_MAXIMUM_FRAME_SIZE//[[:space:]]/}"
AKKA_BUFFER_POOL_SIZE="${AKKA_BUFFER_POOL_SIZE:-4}"
DIS_IND_BATCH_SIZE="${DIS_IND_BATCH_SIZE:-15000}"
DIS_IND_CHUNK_SIZE="${DIS_IND_CHUNK_SIZE:-5000000}"
DIS_IND_DATA_ORIENTATION="${DIS_IND_DATA_ORIENTATION:-value}"
DIS_IND_CANDIDATE_TRACKING="${DIS_IND_CANDIDATE_TRACKING:-count}"
DIS_IND_DATASET_NAME="${DIS_IND_DATASET_NAME:-tpch-1}"
DIS_IND_INGESTION_MODE="${DIS_IND_INGESTION_MODE:-insert}"
DIS_IND_PRUNE_CQF_ENABLED="${DIS_IND_PRUNE_CQF_ENABLED:-true}"
DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED="${DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED:-true}"
DIS_IND_PRUNE_PARTITION_HIERARCHY_ENABLED="${DIS_IND_PRUNE_PARTITION_HIERARCHY_ENABLED:-true}"
DIS_IND_PRUNE_TRANSITIVE_ENABLED="${DIS_IND_PRUNE_TRANSITIVE_ENABLED:-false}"
DIS_IND_PRUNE_COUNT_PARTITIONS="${DIS_IND_PRUNE_COUNT_PARTITIONS:-64}"
DIS_IND_CLUSTER_VALIDATION="${DIS_IND_CLUSTER_VALIDATION:-scan}"
DIS_IND_EXACT_EVENT_FILTERING_ENABLED="${DIS_IND_EXACT_EVENT_FILTERING_ENABLED:-true}"
DIS_IND_EXACT_DIRECT_VIOLATION_ENABLED="${DIS_IND_EXACT_DIRECT_VIOLATION_ENABLED:-true}"
EXPECTED_WORKERS="${#WORKER_HOSTS[@]}"
EXPECTED_MEMBERS="$((EXPECTED_WORKERS + 1))"

remote() {
    local host="$1"
    shift
    ssh "${SSH_USER}@${host}" "$@"
}

update_and_build() {
    local host="$1"
    echo "[$host] Updating $GIT_BRANCH and building"
    remote "$host" bash -s -- "$GIT_URL" "$GIT_BRANCH" "$REMOTE_PROJECT_DIR" <<'REMOTE_BUILD'
set -euo pipefail
git_url="$1"
git_branch="$2"
project_dir="$3"

if [[ -d "$project_dir/.git" ]]; then
    cd "$project_dir"
    current_branch="$(git branch --show-current)"
    if [[ "$current_branch" != "$git_branch" ]]; then
        echo "Expected branch '$git_branch', but '$current_branch' is checked out in $project_dir" >&2
        exit 1
    fi
    git pull --ff-only origin "$git_branch"
elif [[ -e "$project_dir" ]]; then
    echo "$project_dir exists but is not a Git repository" >&2
    exit 1
else
    mkdir -p "$(dirname "$project_dir")"
    git clone --branch "$git_branch" --single-branch "$git_url" "$project_dir"
    cd "$project_dir"
fi

mvn -DskipTests package
test -s target/dis-ind-1.0.0.jar
REMOTE_BUILD
}

ensure_not_running() {
    local host="$1"
    remote "$host" bash -s -- "$REMOTE_STATE_DIR" "$AKKA_PORT" <<'REMOTE_CHECK'
set -euo pipefail
state_dir="$1"
akka_port="$2"
pid_file="$state_dir/application.pid"
if [[ -s "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
        echo "DIS-IND is already running with PID $pid" >&2
        exit 1
    fi
fi
if pgrep -f '[d]isIND.ValueBasedMain' >/dev/null 2>&1; then
    echo "A DIS-IND JVM from another run is still active on this node" >&2
    pgrep -af '[d]isIND.ValueBasedMain' >&2 || true
    exit 1
fi
if command -v ss >/dev/null 2>&1 && \
        ss -ltn | awk -v wanted=":$akka_port" '$4 ~ wanted "$" { found=1 } END { exit !found }'; then
    echo "Akka port $akka_port is already in use on this node" >&2
    exit 1
fi
if [[ -e "$state_dir" ]]; then
    echo "Run-state directory already exists; refusing to reuse it: $state_dir" >&2
    exit 1
fi
REMOTE_CHECK
}

start_node() {
    local host="$1"
    local advertised_ip="$2"
    local role="$3"
    local java_xmx="$4"

    echo "[$host] Starting $role at $advertised_ip:$AKKA_PORT"
    remote "$host" bash -s -- \
        "$REMOTE_PROJECT_DIR" "$REMOTE_STATE_DIR" "$REMOTE_LOG_DIR" \
        "$role" "$advertised_ip" "$COORDINATOR_IP" "$AKKA_PORT" \
        "$EXPECTED_MEMBERS" "$EXPECTED_WORKERS" "$CLUSTER_START_TIMEOUT_SECONDS" \
        "$JAVA_XMS" "$java_xmx" "$COORDINATOR_INPUT_DIR" "$COORDINATOR_OUTPUT_DIR" \
        "$DIS_IND_BATCH_SIZE" "$DIS_IND_CHUNK_SIZE" "$DIS_IND_DATA_ORIENTATION" \
        "$DIS_IND_CANDIDATE_TRACKING" "$DIS_IND_DATASET_NAME" "$DIS_IND_INGESTION_MODE" \
        "$DIS_IND_PRUNE_CQF_ENABLED" "$DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED" \
        "$DIS_IND_PRUNE_PARTITION_HIERARCHY_ENABLED" "$DIS_IND_PRUNE_TRANSITIVE_ENABLED" \
        "$DIS_IND_PRUNE_COUNT_PARTITIONS" "$DIS_IND_CLUSTER_VALIDATION" \
        "$DIS_IND_EXACT_EVENT_FILTERING_ENABLED" "$DIS_IND_EXACT_DIRECT_VIOLATION_ENABLED" \
        "$AKKA_MAXIMUM_FRAME_SIZE" "$AKKA_BUFFER_POOL_SIZE" <<'REMOTE_START'
set -euo pipefail
project_dir="$1"
state_dir="$2"
log_dir="$3"
role="$4"
advertised_ip="$5"
coordinator_ip="$6"
akka_port="$7"
expected_members="$8"
expected_workers="$9"
start_timeout="${10}"
java_xms="${11}"
java_xmx="${12}"
input_dir="${13}"
output_dir="${14}"
batch_size="${15}"
chunk_size="${16}"
data_orientation="${17}"
candidate_tracking="${18}"
dataset_name="${19}"
ingestion_mode="${20}"
prune_cqf_enabled="${21}"
prune_partition_counts_enabled="${22}"
prune_partition_hierarchy_enabled="${23}"
prune_transitive_enabled="${24}"
prune_count_partitions="${25}"
cluster_validation="${26}"
exact_event_filtering_enabled="${27}"
exact_direct_violation_enabled="${28}"
maximum_frame_size="${29}"
buffer_pool_size="${30}"

if [[ -e "$state_dir" ]]; then
    echo "Run-state directory already exists; refusing to reuse it: $state_dir" >&2
    exit 1
fi
mkdir -p "$state_dir/value-ids" "$state_dir/value-to-rows" \
    "$state_dir/value-owners" "$state_dir/diagnostics" "$log_dir"
if [[ "$role" == "coordinator" ]]; then
    test -d "$input_dir" || { echo "Input directory not found: $input_dir" >&2; exit 1; }
    mkdir -p "$output_dir"
fi

cd "$project_dir"
log_file="$log_dir/application.log"
pid_file="$state_dir/application.pid"

common_env=(
    "DIS_IND_NODE_ROLE=$role"
    "DIS_IND_EXPECTED_CLUSTER_SIZE=$expected_members"
    "DIS_IND_EXPECTED_WORKERS=$expected_workers"
    "DIS_IND_CLUSTER_START_TIMEOUT_SECONDS=$start_timeout"
    "AKKA_HOSTNAME=$advertised_ip"
    "AKKA_PORT=$akka_port"
    "AKKA_SEED_HOST=$coordinator_ip"
    "AKKA_SEED_PORT=$akka_port"
    "AKKA_MIN_WORKERS=$expected_workers"
    "AKKA_MAXIMUM_FRAME_SIZE=$maximum_frame_size"
    "AKKA_BUFFER_POOL_SIZE=$buffer_pool_size"
    "DIS_IND_BATCH_SIZE=$batch_size"
    "DIS_IND_CHUNK_SIZE=$chunk_size"
    "DIS_IND_DATA_ORIENTATION=$data_orientation"
    "DIS_IND_CANDIDATE_TRACKING=$candidate_tracking"
    "DIS_IND_DATASET_NAME=$dataset_name"
    "DIS_IND_INGESTION_MODE=$ingestion_mode"
    "DIS_IND_PRUNE_CQF_ENABLED=$prune_cqf_enabled"
    "DIS_IND_PRUNE_PARTITION_COUNTS_ENABLED=$prune_partition_counts_enabled"
    "DIS_IND_PRUNE_PARTITION_HIERARCHY_ENABLED=$prune_partition_hierarchy_enabled"
    "DIS_IND_PRUNE_TRANSITIVE_ENABLED=$prune_transitive_enabled"
    "DIS_IND_PRUNE_COUNT_PARTITIONS=$prune_count_partitions"
    "DIS_IND_CLUSTER_VALIDATION=$cluster_validation"
    "DIS_IND_EXACT_EVENT_FILTERING_ENABLED=$exact_event_filtering_enabled"
    "DIS_IND_EXACT_DIRECT_VIOLATION_ENABLED=$exact_direct_violation_enabled"
    "DIS_IND_DIAGNOSTICS_DIR=$state_dir/diagnostics"
    "DIS_IND_VALUE_ID_DISK_DIR=$state_dir/value-ids"
    "DIS_IND_VALUE_TO_ROWS_DISK_DIR=$state_dir/value-to-rows"
    "DIS_IND_VALUE_OWNER_DISK_DIR=$state_dir/value-owners"
)
if [[ "$role" == "coordinator" ]]; then
    common_env+=("DIS_IND_INPUT_DIR=$input_dir" "DIS_IND_OUTPUT_FILE=$output_dir/ind-report.txt")
fi

nohup env "${common_env[@]}" java \
    "-Xms$java_xms" "-Xmx$java_xmx" \
    "-Dakka.cluster.roles.0=$role" \
    "-Dakka.cluster.seed-nodes.0=akka://disIND@$coordinator_ip:$akka_port" \
    -cp target/dis-ind-1.0.0.jar disIND.ValueBasedMain \
    >"$log_file" 2>&1 </dev/null &
pid="$!"
echo "$pid" > "$pid_file"
sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
    echo "DIS-IND exited during startup; recent log output:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
fi
echo "Started PID $pid; log: $log_file"
REMOTE_START
}

wait_for_coordinator_port() {
    echo "Waiting for coordinator port $COORDINATOR_IP:$AKKA_PORT"
    local attempt
    for ((attempt = 1; attempt <= 120; attempt++)); do
        if remote "$COORDINATOR" bash -s -- "$AKKA_PORT" <<'REMOTE_PORT'
port="$1"
if command -v ss >/dev/null 2>&1; then
    ss -ltn | awk -v wanted=":$port" '$4 ~ wanted "$" { found=1 } END { exit !found }'
else
    nc -z 127.0.0.1 "$port"
fi
REMOTE_PORT
        then
            return 0
        fi
        sleep 1
    done
    echo "Coordinator did not listen on port $AKKA_PORT within 120 seconds" >&2
    echo "Recent coordinator log output:" >&2
    remote "$COORDINATOR" tail -n 120 "$REMOTE_LOG_DIR/application.log" >&2 || true
    return 1
}

collect_worker_diagnostics() {
    local destination="$COORDINATOR_OUTPUT_DIR/nodes"
    echo "Collecting node diagnostics under $COORDINATOR:$destination"
    remote "$COORDINATOR" mkdir -p "$destination/coordinator"
    if remote "$COORDINATOR" test -d "$REMOTE_STATE_DIR"; then
        remote "$COORDINATOR" tar -C "$REMOTE_STATE_DIR" -czf - diagnostics logs | \
            remote "$COORDINATOR" "tar -C $(printf '%q' "$destination/coordinator") -xzf -"
    else
        echo "[$COORDINATOR] No runtime diagnostics were created"
    fi
    local worker
    for worker in "${WORKER_HOSTS[@]}"; do
        local remote_destination
        printf -v remote_destination '%q' "$destination/$worker"
        if remote "$worker" test -d "$REMOTE_STATE_DIR"; then
            remote "$worker" tar -C "$REMOTE_STATE_DIR" -czf - diagnostics logs | \
                remote "$COORDINATOR" "mkdir -p $remote_destination && tar -C $remote_destination -xzf -"
        else
            echo "[$worker] No runtime diagnostics were created"
        fi
    done
}

prepare_cluster() {
    local hosts=("$COORDINATOR" "${WORKER_HOSTS[@]}")
    local build_pids=()
    local host
    for host in "${hosts[@]}"; do
        update_and_build "$host" &
        build_pids+=("$!")
    done
    local pid
    for pid in "${build_pids[@]}"; do
        wait "$pid"
    done

    local expected_commit=""
    for host in "${hosts[@]}"; do
        local commit
        commit="$(remote "$host" git -C "$REMOTE_PROJECT_DIR" rev-parse HEAD)"
        if [[ -z "$expected_commit" ]]; then
            expected_commit="$commit"
        elif [[ "$commit" != "$expected_commit" ]]; then
            echo "Git commit mismatch: $host has $commit; expected $expected_commit" >&2
            return 1
        fi
    done
    echo "All nodes built commit $expected_commit"
}

launch_cluster() {
    local hosts=("$COORDINATOR" "${WORKER_HOSTS[@]}")
    local host
    for host in "${hosts[@]}"; do
        ensure_not_running "$host"
    done

    start_node "$COORDINATOR" "$COORDINATOR_IP" coordinator "$COORDINATOR_JAVA_XMX"
    wait_for_coordinator_port
    local index
    for index in "${!WORKER_HOSTS[@]}"; do
        start_node "${WORKER_HOSTS[$index]}" "${WORKER_ADDRESSES[$index]}" worker "$WORKER_JAVA_XMX"
    done
    echo "Cluster launched: 1 coordinator + $EXPECTED_WORKERS workers"
    echo "Coordinator log: ssh ${SSH_USER}@${COORDINATOR} tail -f $REMOTE_LOG_DIR/application.log"
}

wait_for_node_exit() {
    local host="$1"
    local timeout_seconds="$2"
    remote "$host" bash -s -- "$REMOTE_STATE_DIR" "$timeout_seconds" <<'REMOTE_WAIT'
set -euo pipefail
state_dir="$1"
timeout_seconds="$2"
pid_file="$state_dir/application.pid"
test -s "$pid_file" || { echo "Missing PID file: $pid_file" >&2; exit 1; }
pid="$(cat "$pid_file")"
deadline=$((SECONDS + timeout_seconds))
while kill -0 "$pid" 2>/dev/null; do
    process_state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
    [[ "$process_state" == Z* ]] && break
    if (( SECONDS >= deadline )); then
        echo "Timed out waiting for PID $pid" >&2
        exit 124
    fi
    sleep 2
done
REMOTE_WAIT
}

wait_for_cluster() {
    local timeout_seconds="${EXPERIMENT_TIMEOUT_SECONDS:-21600}"
    echo "Waiting up to $timeout_seconds seconds for the coordinator"
    wait_for_node_exit "$COORDINATOR" "$timeout_seconds"

    local worker
    for worker in "${WORKER_HOSTS[@]}"; do
        echo "Waiting for worker $worker to stop"
        wait_for_node_exit "$worker" 180
    done

    if ! remote "$COORDINATOR" test -s "$COORDINATOR_OUTPUT_DIR/ind-report.txt"; then
        echo "Coordinator completed without a non-empty ind-report.txt" >&2
        return 1
    fi
}

stop_node() {
    local host="$1"
    remote "$host" bash -s -- "$REMOTE_STATE_DIR" <<'REMOTE_STOP'
set -euo pipefail
state_dir="$1"
pid_file="$state_dir/application.pid"
[[ -s "$pid_file" ]] || exit 0
pid="$(cat "$pid_file")"
kill -0 "$pid" 2>/dev/null || exit 0
command_line="$(ps -o command= -p "$pid" 2>/dev/null || true)"
if [[ "$command_line" != *disIND.ValueBasedMain* ]]; then
    echo "Refusing to stop PID $pid because it is not DIS-IND: $command_line" >&2
    exit 1
fi
kill "$pid"
for _ in {1..10}; do
    kill -0 "$pid" 2>/dev/null || exit 0
    sleep 1
done
kill -KILL "$pid" 2>/dev/null || true
REMOTE_STOP
}

stop_cluster() {
    local worker
    for worker in "${WORKER_HOSTS[@]}"; do
        stop_node "$worker" || true
    done
    stop_node "$COORDINATOR" || true
}

case "${1:-start}" in
    deploy|start)
        prepare_cluster
        launch_cluster
        ;;
    prepare)
        prepare_cluster
        ;;
    launch)
        launch_cluster
        ;;
    wait)
        wait_for_cluster
        ;;
    stop)
        stop_cluster
        ;;
    collect)
        collect_worker_diagnostics
        ;;
    *)
        echo "Usage: $0 [deploy|start|prepare|launch|wait|stop|collect]" >&2
        exit 2
        ;;
esac
