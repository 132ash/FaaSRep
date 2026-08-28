#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SOURCE="$SCRIPT_DIR/../../../actual_apps/test7_colocate_apps/trace/prepare/segments"

WORKFLOW="${WORKFLOW:-c4}"
SYSTEM_MODE="${SYSTEM_MODE:-hybrid}"
TRACE="${TRACE:-lowload}"
SEGMENTS_ROOT="${SEGMENTS_ROOT:-$DEFAULT_SOURCE}"
ZIPF_PARAM="${ZIPF_PARAM:-0.9}"
RETRY_ABORT_SEED="${RETRY_ABORT_SEED:-20260827}"
ABORT_PROBS_OVERRIDE="${ABORT_PROBS_OVERRIDE:-0 0.25 0.50 0.75 1.00}"

if [ "$TRACE" = "highload" ]; then
    DEFAULT_SEGMENTS="1 6 17 24 26"
else
    DEFAULT_SEGMENTS="1 8 17 19 29"
fi
TARGET_SEGMENT_INDICES_OVERRIDE="${TARGET_SEGMENT_INDICES_OVERRIDE:-$DEFAULT_SEGMENTS}"

read -r -a ABORT_PROBS <<< "$ABORT_PROBS_OVERRIDE"
read -r -a TARGET_SEGMENT_INDICES <<< "$TARGET_SEGMENT_INDICES_OVERRIDE"

for ABORT_PROB in "${ABORT_PROBS[@]}"; do
    for SEGMENT_INDEX in "${TARGET_SEGMENT_INDICES[@]}"; do
        SEGMENT_FILE="$SEGMENTS_ROOT/$TRACE/segment_${SEGMENT_INDEX}.json"
        if [ ! -f "$SEGMENT_FILE" ]; then
            echo "segment file not found: $SEGMENT_FILE" >&2
            exit 1
        fi
        echo "Running trace=$TRACE segment=$SEGMENT_INDEX abort_prob=$ABORT_PROB"
        python3 "$SCRIPT_DIR/run_segment.py" \
            --segment "$SEGMENT_FILE" \
            --trace "$TRACE" \
            --workflow "$WORKFLOW" \
            --system-mode "$SYSTEM_MODE" \
            --zipf "$ZIPF_PARAM" \
            --abort-prob "$ABORT_PROB" \
            --seed "$RETRY_ABORT_SEED"
    done
done
