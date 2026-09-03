#!/usr/bin/env bash

set -euo pipefail

SSH_USER="${SSH_USER:-node}"
CLUSTER_HOSTS_TEXT="${CLUSTER_HOSTS:-192.168.2.139 192.168.2.233 192.168.2.232 192.168.2.90 192.168.2.138 192.168.2.89 192.168.2.157}"
read -r -a CLUSTER_HOSTS_ARRAY <<< "$CLUSTER_HOSTS_TEXT"

check_node() {
    local host="$1"
    ssh -o BatchMode=yes -o ConnectTimeout=10 "$SSH_USER@$host" bash -s <<'REMOTE_CHECK'
set -euo pipefail
missing=0
for command in git mvn java javac; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "MISSING: $command"
        missing=1
    fi
done

if command -v java >/dev/null 2>&1; then
    java_version="$(java -version 2>&1 | head -n 1)"
    echo "java: $java_version"
    java_major="$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/ {print $2; exit}')"
    if [[ "$java_major" != "21" ]]; then
        echo "WRONG VERSION: Java 21 is required, found $java_major"
        missing=1
    fi
fi
command -v git >/dev/null 2>&1 && echo "git: $(git --version)"
command -v mvn >/dev/null 2>&1 && echo "maven: $(mvn --version | sed -n '1p')"
exit "$missing"
REMOTE_CHECK
}

install_node() {
    local host="$1"
    echo "[$host] Installing Git, Maven and OpenJDK 21 (sudo may prompt)"
    ssh -tt -o ConnectTimeout=10 "$SSH_USER@$host" \
        'sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y git maven openjdk-21-jdk && arch="$(dpkg --print-architecture)" && java_home="/usr/lib/jvm/java-21-openjdk-${arch}" && sudo update-alternatives --set java "$java_home/bin/java" && sudo update-alternatives --set javac "$java_home/bin/javac"'
}

failed=0
for host in "${CLUSTER_HOSTS_ARRAY[@]}"; do
    echo
    echo "[$host] Checking dependencies"
    if check_node "$host"; then
        echo "[$host] Dependencies already satisfy requirements"
        continue
    fi

    if ! install_node "$host"; then
        echo "[$host] Installation failed" >&2
        failed=1
        continue
    fi

    if check_node "$host"; then
        echo "[$host] Installation verified"
    else
        echo "[$host] Dependencies still do not satisfy requirements" >&2
        failed=1
    fi
done

echo
if (( failed != 0 )); then
    echo "Dependency setup failed on one or more nodes." >&2
    exit 1
fi

echo "Git, Maven and Java 21 are ready on all ${#CLUSTER_HOSTS_ARRAY[@]} nodes."
