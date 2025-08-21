echo "Build images for workflow travel_reservation"

SCRIPT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)

docker build --no-cache -t reserve_flight $SCRIPT_DIR/../../../benchmark/travel_reservation/reserve_flight
docker build --no-cache -t reserve_car_rental $SCRIPT_DIR/../../../benchmark/travel_reservation/reserve_car_rental
docker build --no-cache -t confirm_reservation $SCRIPT_DIR/../../../benchmark/travel_reservation/confirm_reservation
docker build --no-cache -t payment $SCRIPT_DIR/../../../benchmark/travel_reservation/payment

