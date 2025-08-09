import json

def main():
    func_input = store.fetch_input()
    transaction_id = func_input["transaction_id"]
    flight_reservation_id = func_input["flight_reservation_id"]
    car_reservation_id = func_input["car_reservation_id"]
    payment_id_suffix = hash(f"{flight_reservation_id}_{car_reservation_id}") % 1000000
    payment_id = f"{transaction_id}_{payment_id_suffix}"
    store.put(payment_id, json.dumps({'id': payment_id, 'payment':750}))
    store.ret({"car_reservation_id":car_reservation_id, 'flight_reservation_id': flight_reservation_id, 'payment_id': payment_id})