#!/bin/bash

# Specify workflow name
WORKFLOW="banking_system"
SYSTEM="pessimistic"

# Set paths
TRACE_DIR="/home/shao/FaaSnap/experiment/actual_apps/test7_colocate_apps/trace"
SEGMENTS_DIR="$TRACE_DIR/segments"
RESULT_DIR="$TRACE_DIR/results_segments/$SYSTEM/$WORKFLOW"

mkdir -p "$RESULT_DIR"

echo "Running experiment for workflow: $WORKFLOW"
echo "Segments directory: $SEGMENTS_DIR"
echo "Results directory: $RESULT_DIR"

# Specify segment indices
TARGET_SEGMENT_INDICES=(0 1 2 3 4 5 6)

for TARGET_SEGMENT_IDX in "${TARGET_SEGMENT_INDICES[@]}"; do
    SEGMENT_FILE="$SEGMENTS_DIR/segment_${TARGET_SEGMENT_IDX}.json"
    OUTPUT_FILE="$RESULT_DIR/result_segment_${TARGET_SEGMENT_IDX}.json"

    if [ -f "$SEGMENT_FILE" ]; then
        echo "Running segment $TARGET_SEGMENT_IDX..."
        python3 "$TRACE_DIR/run_segment.py" --segment "$SEGMENT_FILE" --output "$OUTPUT_FILE"
    else
        echo "Error: Segment file $SEGMENT_FILE not found."
    fi
done

echo "All segments finished."
