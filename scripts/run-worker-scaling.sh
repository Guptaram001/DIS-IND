#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$SCRIPT_DIR/../ExperimentConfig/worker-scaling.yaml"
if [[ $# -gt 0 && "$1" != -* ]]; then
    CONFIG="$1"
    shift
fi
exec python3 -B "$SCRIPT_DIR/run-worker-scaling.py" "$CONFIG" "$@"
