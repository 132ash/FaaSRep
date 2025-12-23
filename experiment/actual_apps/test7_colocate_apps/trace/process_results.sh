#!/bin/bash

# Specify workflow name
WORKFLOW="social_network"
SYSTEM="pessimistic"
# optimistic

# Set paths
TRACE_DIR="/home/shao/FaaSnap/experiment/actual_apps/test7_colocate_apps/trace"
RESULT_SEGMENTS_DIR="$TRACE_DIR/results_segments/$SYSTEM/$WORKFLOW"
MERGED_RESULT="$TRACE_DIR/result/$SYSTEM/${WORKFLOW}_merged.json"
CSV_OUTPUT="$TRACE_DIR/result/$SYSTEM/${WORKFLOW}_output.csv"

mkdir -p "$(dirname "$MERGED_RESULT")"

# Merge results
echo "Merging results for $WORKFLOW..."
python3 "$TRACE_DIR/merge_results.py" --result-dir "$RESULT_SEGMENTS_DIR" --output "$MERGED_RESULT"

# Analyze results
echo "Analyzing results..."
python3 "$TRACE_DIR/analyze_results.py" --file "$MERGED_RESULT" --output-csv "$CSV_OUTPUT" --workflow "$WORKFLOW"

echo "Done! Results saved to $CSV_OUTPUT"
