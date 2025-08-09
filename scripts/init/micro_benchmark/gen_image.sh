echo "Build images for microbenchmark_func"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t micro_func $SCRIPT_DIR/microbenchmark_func

