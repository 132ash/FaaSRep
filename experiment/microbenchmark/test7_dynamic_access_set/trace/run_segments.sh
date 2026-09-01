#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SOURCE="$SCRIPT_DIR/../../../actual_apps/test7_colocate_apps/trace/prepare/segments"

WORKFLOW="${WORKFLOW:-c4}"
TRACE="${TRACE:-lowload}"
SEGMENTS_ROOT="${SEGMENTS_ROOT:-$DEFAULT_SOURCE}"
ZIPF_PARAM="${ZIPF_PARAM:-0.9}"
SEED="${SEED:-20260827}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-300}"
TARGET_SEGMENT_INDICES_OVERRIDE="${TARGET_SEGMENT_INDICES_OVERRIDE:-5}"

read -r -a TARGET_SEGMENT_INDICES <<< "$TARGET_SEGMENT_INDICES_OVERRIDE"

for SEGMENT_INDEX in "${TARGET_SEGMENT_INDICES[@]}"; do
    SEGMENT_FILE="$SEGMENTS_ROOT/$TRACE/segment_${SEGMENT_INDEX}.json"
    if [ ! -f "$SEGMENT_FILE" ]; then
        echo "segment file not found: $SEGMENT_FILE" >&2
        exit 1
    fi
    echo "Running OCC trace=$TRACE segment=$SEGMENT_INDEX"
    python3 "$SCRIPT_DIR/run_segment.py" \
        --segment "$SEGMENT_FILE" \
        --trace "$TRACE" \
        --workflow "$WORKFLOW" \
        --zipf "$ZIPF_PARAM" \
        --seed "$SEED" \
        --request-timeout "$REQUEST_TIMEOUT"
done
