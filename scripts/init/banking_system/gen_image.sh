echo "Build images for workflow travel_reservation"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t banking_login $SCRIPT_DIR/../../../benchmark/banking_system/banking_login
docker build --no-cache -t withdraw $SCRIPT_DIR/../../../benchmark/banking_system/withdraw
docker build --no-cache -t deposit $SCRIPT_DIR/../../../benchmark/banking_system/deposit