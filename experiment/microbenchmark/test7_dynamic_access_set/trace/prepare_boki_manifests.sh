#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SOURCE="$SCRIPT_DIR/../../../actual_apps/test7_colocate_apps/trace/prepare/segments"

TRACE="${TRACE:-lowload}"
SEGMENTS_ROOT="${SEGMENTS_ROOT:-$DEFAULT_SOURCE}"
MANIFEST_ROOT="${MANIFEST_ROOT:-$SCRIPT_DIR/manifests}"
ZIPF_PARAM="${ZIPF_PARAM:-0.9}"
SEED="${SEED:-20260827}"
TARGET_SEGMENT_INDICES_OVERRIDE="${TARGET_SEGMENT_INDICES_OVERRIDE:-5}"

read -r -a TARGET_SEGMENT_INDICES <<< "$TARGET_SEGMENT_INDICES_OVERRIDE"
for SEGMENT_INDEX in "${TARGET_SEGMENT_INDICES[@]}"; do
    SEGMENT_FILE="$SEGMENTS_ROOT/$TRACE/segment_${SEGMENT_INDEX}.json"
    OUTPUT_FILE="$MANIFEST_ROOT/$TRACE/c4_zipf${ZIPF_PARAM}_segment_${SEGMENT_INDEX}.jsonl"
    if [ ! -f "$SEGMENT_FILE" ]; then
        echo "segment file not found: $SEGMENT_FILE" >&2
        exit 1
    fi
    echo "Preparing Boki-SN manifest trace=$TRACE segment=$SEGMENT_INDEX"
    python3 "$SCRIPT_DIR/prepare_boki_manifests.py" \
        --segment "$SEGMENT_FILE" --output "$OUTPUT_FILE" --trace "$TRACE" \
        --zipf "$ZIPF_PARAM" --seed "$SEED"
done
