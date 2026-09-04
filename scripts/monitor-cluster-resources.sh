#!/usr/bin/env bash

set -euo pipefail

SSH_USER="${SSH_USER:-node}"
CLUSTER_HOSTS_TEXT="${CLUSTER_HOSTS:-192.168.2.139 192.168.2.233 192.168.2.232 192.168.2.90 192.168.2.138 192.168.2.89 192.168.2.157}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-5}"
WAIT_START_SECONDS="${WAIT_START_SECONDS:-600}"
CPU_CORES="${CPU_CORES:-4}"
OUTPUT_ROOT="${OUTPUT_ROOT:-cluster-monitor}"

read -r -a CLUSTER_HOSTS_ARRAY <<< "$CLUSTER_HOSTS_TEXT"

case "$SAMPLE_SECONDS:$WAIT_START_SECONDS:$CPU_CORES" in
    *[!0-9:]*|0:*|*:0:*|*:0)
        echo "SAMPLE_SECONDS, WAIT_START_SECONDS and CPU_CORES must be positive integers" >&2
        exit 2
        ;;
esac

run_id="$(date +%Y%m%d-%H%M%S)"
output_dir="$OUTPUT_ROOT/$run_id"
mkdir -p "$output_dir"

sample_node() {
    local host="$1"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$SSH_USER@$host" \
        bash -s -- "$SAMPLE_SECONDS" "$WAIT_START_SECONDS" <<'REMOTE_SAMPLE'
set -euo pipefail
interval="$1"
wait_seconds="$2"

deadline=$((SECONDS + wait_seconds))
pid=""
while (( SECONDS < deadline )); do
    pid="$(pgrep -f '[d]isIND.ValueBasedMain' | head -n 1 || true)"
    [[ -n "$pid" ]] && break
    sleep 1
done

if [[ -z "$pid" ]]; then
    echo "ERROR: DIS-IND did not start within $wait_seconds seconds" >&2
    exit 3
fi

clock_ticks="$(getconf CLK_TCK)"
page_size="$(getconf PAGESIZE)"
printf 'timestamp\tjava_cpu_percent\tiowait_percent\trss_mib\tread_mib_s\twrite_mib_s\tvoluntary_ctx_s\tinvoluntary_ctx_s\tload1\trunnable\n'

read_cpu() {
    awk '/^cpu / { total=0; for (i=2; i<=NF; i++) total+=$i; print total, $6; exit }' /proc/stat
}

read_process() {
    local stat_line after state_rest utime stime rss_pages read_bytes write_bytes voluntary involuntary
    stat_line="$(<"/proc/$pid/stat")"
    after="${stat_line#*) }"
    read -r -a fields <<< "$after"
    # fields[] starts at process stat field 3 (state).
    utime="${fields[11]}"
    stime="${fields[12]}"
    rss_pages="${fields[21]}"
    read_bytes="$(awk '/^read_bytes:/ {print $2}' "/proc/$pid/io")"
    write_bytes="$(awk '/^write_bytes:/ {print $2}' "/proc/$pid/io")"
    voluntary="$(awk '/^voluntary_ctxt_switches:/ {print $2}' "/proc/$pid/status")"
    involuntary="$(awk '/^nonvoluntary_ctxt_switches:/ {print $2}' "/proc/$pid/status")"
    printf '%s %s %s %s %s %s\n' "$((utime + stime))" "$rss_pages" \
        "${read_bytes:-0}" "${write_bytes:-0}" "${voluntary:-0}" "${involuntary:-0}"
}

read -r previous_total previous_iowait < <(read_cpu)
read -r previous_proc _ previous_read previous_write previous_voluntary previous_involuntary < <(read_process)
previous_time="$(date +%s%N)"

while kill -0 "$pid" 2>/dev/null; do
    sleep "$interval"
    kill -0 "$pid" 2>/dev/null || break

    now_time="$(date +%s%N)"
    read -r total iowait < <(read_cpu)
    read -r proc rss_pages read_bytes write_bytes voluntary involuntary < <(read_process)
    read -r load1 _ < /proc/loadavg
    runnable="$(awk '{split($4,a,"/"); print a[1]}' /proc/loadavg)"

    awk -v ts="$(date -Is)" -v dt_ns="$((now_time - previous_time))" \
        -v dp="$((proc - previous_proc))" -v hz="$clock_ticks" \
        -v dtotal="$((total - previous_total))" -v diowait="$((iowait - previous_iowait))" \
        -v rss_pages="$rss_pages" -v page_size="$page_size" \
        -v dread="$((read_bytes - previous_read))" -v dwrite="$((write_bytes - previous_write))" \
        -v dvol="$((voluntary - previous_voluntary))" \
        -v dinvol="$((involuntary - previous_involuntary))" \
        -v load1="$load1" -v runnable="$runnable" 'BEGIN {
            seconds=dt_ns/1000000000;
            cpu=(seconds>0 ? (dp/hz)/seconds*100 : 0);
            io=(dtotal>0 ? diowait/dtotal*100 : 0);
            rss=rss_pages*page_size/1048576;
            read_rate=(seconds>0 ? dread/1048576/seconds : 0);
            write_rate=(seconds>0 ? dwrite/1048576/seconds : 0);
            vol_rate=(seconds>0 ? dvol/seconds : 0);
            invol_rate=(seconds>0 ? dinvol/seconds : 0);
            printf "%s\t%.2f\t%.2f\t%.2f\t%.3f\t%.3f\t%.2f\t%.2f\t%.2f\t%d\n",
                ts,cpu,io,rss,read_rate,write_rate,vol_rate,invol_rate,load1,runnable;
        }'

    previous_time="$now_time"
    previous_total="$total"
    previous_iowait="$iowait"
    previous_proc="$proc"
    previous_read="$read_bytes"
    previous_write="$write_bytes"
    previous_voluntary="$voluntary"
    previous_involuntary="$involuntary"
done
REMOTE_SAMPLE
}

echo "Monitoring ${#CLUSTER_HOSTS_ARRAY[@]} nodes every ${SAMPLE_SECONDS}s"
echo "Waiting up to ${WAIT_START_SECONDS}s for DIS-IND; results: $output_dir"

pids=()
for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    sample_node "$host" >"$output_dir/$host.tsv" 2>"$output_dir/$host.err" &
    pids+=("$!")
done

monitor_failed=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        monitor_failed=1
    fi
done

summary="$output_dir/summary.tsv"
printf 'node\tsamples\tavg_java_cpu_percent\tpeak_java_cpu_percent\tavg_iowait_percent\tpeak_rss_mib\tavg_read_mib_s\tavg_write_mib_s\tavg_involuntary_ctx_s\tavg_load1\tpeak_runnable\n' > "$summary"

for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    file="$output_dir/$host.tsv"
    if [[ ! -s "$file" ]] || (( "$(wc -l < "$file")" <= 1 )); then
        printf '%s\t0\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n' "$host" >> "$summary"
        continue
    fi
    awk -F '\t' -v node="$host" 'NR>1 {
        n++; cpu+=$2; if ($2>maxcpu) maxcpu=$2; io+=$3;
        if ($4>maxrss) maxrss=$4; rd+=$5; wr+=$6; invol+=$8; load+=$9;
        if ($10>maxrun) maxrun=$10;
    } END {
        printf "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.3f\t%.3f\t%.2f\t%.2f\t%d\n",
            node,n,cpu/n,maxcpu,io/n,maxrss,rd/n,wr/n,invol/n,load/n,maxrun;
    }' "$file" >> "$summary"
done

assessment="$output_dir/assessment.txt"
awk -F '\t' -v cores="$CPU_CORES" -v coordinator="${CLUSTER_HOSTS_ARRAY[0]}" 'NR==1 {next} $2==0 {
    print $1 ": NO DATA (see " $1 ".err)"; next
} {
    capacity=cores*100;
    if ($1==coordinator && $5<10)
        verdict="coordinator CPU is not expected to be saturated; evaluate worker rows for dispatcher sizing";
    else if ($5>=10)
        verdict="I/O-bound: average iowait is high; more actor threads are unlikely to help";
    else if ($3<capacity*0.50)
        verdict="CPU underutilized: inspect coordinator/back-pressure/network before adding threads";
    else if ($3<capacity*0.75)
        verdict="moderate CPU utilization: test one additional compute thread and compare runtime";
    else
        verdict="good CPU utilization";
    if ($10>cores*1.5 && $3>=capacity*0.75)
        verdict=verdict "; high load suggests possible oversubscription";
    printf "%s: %s (avg CPU %.1f%% of %.0f%% capacity, iowait %.1f%%, load %.2f)\n",
        $1,verdict,$3,capacity,$5,$10;
}' "$summary" > "$assessment"

echo
column -t -s $'\t' "$summary" 2>/dev/null || cat "$summary"
echo
cat "$assessment"
echo
echo "Raw samples: $output_dir/<node>.tsv"
echo "Summary:     $summary"
echo "Assessment:  $assessment"

exit "$monitor_failed"
