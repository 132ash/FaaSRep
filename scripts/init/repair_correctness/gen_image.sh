echo "Build images for workflow repair_correctness"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
BENCHMARK_DIR="$SCRIPT_DIR/../../../experiment/debug_tests/repair_correctness/benchmark"

docker build --no-cache -t repair_correctness_claim "$BENCHMARK_DIR/claim"
docker build --no-cache -t repair_correctness_use_ryw "$BENCHMARK_DIR/use_ryw"
docker build --no-cache -t repair_correctness_guard_abort "$BENCHMARK_DIR/guard_abort"
docker build --no-cache -t repair_correctness_aggregate "$BENCHMARK_DIR/aggregate"
