#!/bin/bash

# Set paths
TRACE_DIR="/home/shao/FaaSnap/experiment/actual_apps/test7_colocate_apps/trace"
RESULT_DIR="$TRACE_DIR/results_segments"
MERGED_RESULT="$TRACE_DIR/result/merged_trace_1.json"

mkdir -p "$RESULT_DIR"
mkdir -p "$(dirname "$MERGED_RESULT")"

# 1. Split the trace
# echo "Splitting trace..."
# python3 "$TRACE_DIR/split_trace.py"

# 2. Run segments
# Specify the segment index to run
TARGET_SEGMENT_IDX=0  # Change this value to run a different segment

SEGMENT_FILE="$TRACE_DIR/segments/segment_${TARGET_SEGMENT_IDX}.json"
OUTPUT_FILE="$RESULT_DIR/result_segment_${TARGET_SEGMENT_IDX}.json"

if [ -f "$SEGMENT_FILE" ]; then
    echo "Running segment $TARGET_SEGMENT_IDX..."
    python3 "$TRACE_DIR/run_segment.py" --segment "$SEGMENT_FILE" --output "$OUTPUT_FILE"
else
    echo "Error: Segment file $SEGMENT_FILE not found."
    exit 1
fi

# 3. Merge results
# echo "Merging results..."
# python3 "$TRACE_DIR/merge_results.py" --result-dir "$RESULT_DIR" --output "$MERGED_RESULT"

echo "Done! Merged result saved to $MERGED_RESULT"
