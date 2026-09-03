#!/usr/bin/env bash

set -euo pipefail

SSH_USER="${SSH_USER:-node}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
CLUSTER_HOSTS_TEXT="${CLUSTER_HOSTS:-192.168.2.139 192.168.2.233 192.168.2.232 192.168.2.90 192.168.2.138 192.168.2.89 192.168.2.157}"
read -r -a CLUSTER_HOSTS_ARRAY <<< "$CLUSTER_HOSTS_TEXT"

if [[ ! -f "$SSH_KEY" || ! -f "$SSH_KEY.pub" ]]; then
    echo "SSH key not found: $SSH_KEY" >&2
    echo "Create it first with: ssh-keygen -t ed25519" >&2
    exit 1
fi

if ! command -v ssh-copy-id >/dev/null 2>&1; then
    echo "ssh-copy-id is not installed" >&2
    exit 1
fi

echo "Installing $SSH_KEY.pub for user '$SSH_USER'."
echo "You may be asked for each node's password once."

for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    echo
    echo "[$host] Installing SSH key"
    ssh-copy-id \
        -i "$SSH_KEY.pub" \
        -o StrictHostKeyChecking=accept-new \
        "$SSH_USER@$host"
done

echo
echo "Verifying passwordless SSH access"
failed=0
for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    if ssh \
        -i "$SSH_KEY" \
        -o BatchMode=yes \
        -o ConnectTimeout=5 \
        "$SSH_USER@$host" \
        'printf "OK host=%s user=%s\n" "$(hostname)" "$(whoami)"'
    then
        :
    else
        echo "[$host] Passwordless SSH verification failed" >&2
        failed=1
    fi
done

if (( failed != 0 )); then
    echo "One or more nodes failed verification; fix them before running experiments." >&2
    exit 1
fi

echo "Passwordless SSH is ready on all ${#CLUSTER_HOSTS_ARRAY[@]} nodes."
