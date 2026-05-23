#!/bin/bash

# Specify workflow name
WORKFLOW="${WORKFLOW:-social_network}"
SYSTEM="${SYSTEM:-pessimistic}"
TRACE="${TRACE:-lowload}"
COLLECT_BREAKDOWN="${COLLECT_BREAKDOWN:-0}"
# optimistic

# Set paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULT_SEGMENTS_DIR="$RESULT_DIR/segment_result/$TRACE/$SYSTEM/$WORKFLOW"
MERGED_RESULT="$RESULT_DIR/summary/$TRACE/$SYSTEM/${WORKFLOW}_merged.json"
CSV_OUTPUT="$RESULT_DIR/summary/$TRACE/$SYSTEM/${WORKFLOW}_output.csv"
BREAKDOWN_CSV_OUTPUT="$RESULT_DIR/summary/$TRACE/$SYSTEM/${WORKFLOW}_latency_breakdown.csv"

mkdir -p "$(dirname "$MERGED_RESULT")"

# Merge results
echo "Merging results for $WORKFLOW..."
python3 "$SCRIPT_DIR/merge_results.py" --result-dir "$RESULT_SEGMENTS_DIR" --output "$MERGED_RESULT" --default-warmup-seconds 30

# Analyze results
echo "Analyzing results..."
if [ "$COLLECT_BREAKDOWN" = "1" ]; then
    python3 "$SCRIPT_DIR/analyze_results.py" --file "$MERGED_RESULT" --output-csv "$CSV_OUTPUT" --breakdown-csv "$BREAKDOWN_CSV_OUTPUT" --include-breakdown --workflow "$WORKFLOW"
else
    python3 "$SCRIPT_DIR/analyze_results.py" --file "$MERGED_RESULT" --output-csv "$CSV_OUTPUT" --workflow "$WORKFLOW"
fi

echo "Done! Results saved to $CSV_OUTPUT"
if [ "$COLLECT_BREAKDOWN" = "1" ]; then
    echo "Latency breakdown saved to $BREAKDOWN_CSV_OUTPUT"
fi
