#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 4 ]]; then
    echo "Usage: $0 INPUT_DIR OUTPUT_DIR DATASET_NAME CHUNK_CELLS" >&2
    echo "Build first: mvn -Dmaven.test.skip=true package" >&2
    echo "OUTPUT_DIR must be new and outside INPUT_DIR. Keep original inputs for metadata." >&2
    exit 2
fi
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
jar="${DIS_IND_JAR:-$script_dir/../target/dis-ind-1.0.0.jar}"
[[ -f "$jar" ]] || { echo "Build the application first; jar missing: $jar" >&2; exit 1; }
exec java -cp "$jar" disIND.valueBased.dataset.CsvShardWriter "$@"
