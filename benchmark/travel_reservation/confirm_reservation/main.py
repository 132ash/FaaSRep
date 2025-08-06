import json

def main():
    func_input = store.fetch_input()
    flight_reservation_id = func_input["flight_reservation_id"]
    car_reservation_id = func_input["car_reservation_id"]
    payment_id = func_input["payment_id"]
    store.put(flight_reservation_id, 'Confirmed')
    store.put(car_reservation_id, 'Confirmed')
    store.ret({"car_reservation_id":car_reservation_id, 'flight_reservation_id': flight_reservation_id, 'payment_id': payment_id})