#!/usr/bin/env bash

set -euo pipefail

SSH_USER="${SSH_USER:-node}"
GIT_URL="${GIT_URL:-https://github.com/Guptaram001/DIS-IND.git}"
CLUSTER_HOSTS_TEXT="${CLUSTER_HOSTS:-192.168.2.139 192.168.2.233 192.168.2.232 192.168.2.90 192.168.2.138 192.168.2.89 192.168.2.157}"
read -r -a CLUSTER_HOSTS_ARRAY <<< "$CLUSTER_HOSTS_TEXT"

failed=0
for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    echo "[$host] Testing Git repository access"
    if ssh \
        -o BatchMode=yes \
        -o ConnectTimeout=10 \
        "$SSH_USER@$host" \
        git ls-remote "$GIT_URL" HEAD
    then
        echo "[$host] OK"
    else
        echo "[$host] FAILED" >&2
        failed=1
    fi
done

if (( failed != 0 )); then
    echo "Git repository access failed on one or more cluster nodes." >&2
    exit 1
fi

echo "Git repository access is available on all ${#CLUSTER_HOSTS_ARRAY[@]} nodes."
