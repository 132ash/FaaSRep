#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TEST_DIR="$ROOT_DIR/experiment/debug_tests/repair_correctness"
PYTHON_BIN="${PYTHON_BIN:-python}"

CONCURRENCY="${CONCURRENCY:-32}"
STRESS_CONCURRENCY="${STRESS_CONCURRENCY:-32}"
DURATION_S="${DURATION_S:-120}"
STRESS_KEY_GROUPS="${STRESS_KEY_GROUPS:-4}"
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-60}"
STAGGER_S="${STAGGER_S:-0.1}"

cd "$ROOT_DIR"

RUN_ID="${FAASNAP_LOG_RUN_ID:-$(date +%Y%m%d_%H%M%S)_$$}"
export FAASNAP_LOG_RUN_ID="$RUN_ID"
mkdir -p "$ROOT_DIR/logging/runs/$RUN_ID"
RESULTS_FILE="${RESULTS_FILE:-$ROOT_DIR/logging/runs/$RUN_ID/latest_results.json}"
printf '%s\n' "$RUN_ID" > "$ROOT_DIR/logging/.active_run_id"
printf '%s\n' "bootstrap" > "$ROOT_DIR/logging/.active_experiment"
echo "Using log run id: $RUN_ID"
echo "Logs directory: $ROOT_DIR/logging/runs/$RUN_ID"
echo "Results file: $RESULTS_FILE"
echo "Parameters: concurrency=$CONCURRENCY stress_concurrency=$STRESS_CONCURRENCY duration_s=$DURATION_S stress_key_groups=$STRESS_KEY_GROUPS request_timeout=$REQUEST_TIMEOUT stagger_s=$STAGGER_S"

"$PYTHON_BIN" "$TEST_DIR/run_gateway_suite.py" \
  --output "$RESULTS_FILE" \
  --concurrency "$CONCURRENCY" \
  --stress-concurrency "$STRESS_CONCURRENCY" \
  --duration-s "$DURATION_S" \
  --stress-key-groups "$STRESS_KEY_GROUPS" \
  --request-timeout "$REQUEST_TIMEOUT" \
  --stagger-s "$STAGGER_S"
"$PYTHON_BIN" "$TEST_DIR/verify_dynamodb.py" --results "$RESULTS_FILE"
