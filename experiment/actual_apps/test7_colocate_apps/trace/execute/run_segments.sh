#!/bin/bash

# Specify workflow name
WORKFLOW="${WORKFLOW:-social_network}"
SYSTEM="${SYSTEM:-pessimistic}"
TRACE="${TRACE:-lowload}"

# Set paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SEGMENTS_DIR="$TRACE_DIR/prepare/segments/$TRACE"
RESULT_DIR="$TRACE_DIR/result/segment_result/$TRACE/$SYSTEM/$WORKFLOW"

mkdir -p "$RESULT_DIR"

echo "Running experiment for workflow: $WORKFLOW"
echo "Segments directory: $SEGMENTS_DIR"
echo "Results directory: $RESULT_DIR"

# Specify segment indices
# Recommended representative 2-minute segment IDs:
#   highload: 1 6 17 24 26
#   lowload:  1 8 17 19 29
#
# Override at runtime, for example:
#   TRACE=lowload TARGET_SEGMENT_INDICES_OVERRIDE="1 8 17 19 29" bash execute/run_segments.sh
TARGET_SEGMENT_INDICES=(29)


for TARGET_SEGMENT_IDX in "${TARGET_SEGMENT_INDICES[@]}"; do
    SEGMENT_FILE="$SEGMENTS_DIR/segment_${TARGET_SEGMENT_IDX}.json"
    OUTPUT_FILE="$RESULT_DIR/result_segment_${TARGET_SEGMENT_IDX}.json"

    if [ -f "$SEGMENT_FILE" ]; then
        echo "Running segment $TARGET_SEGMENT_IDX..."
        python3 "$SCRIPT_DIR/run_segment.py" --segment "$SEGMENT_FILE" --output "$OUTPUT_FILE"
    else
        echo "Error: Segment file $SEGMENT_FILE not found."
    fi
done

echo "All segments finished."
