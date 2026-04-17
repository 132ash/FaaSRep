from datetime import datetime, timedelta
import random

def main():
    func_input = store.fetch_input()
    transaction_id = func_input["transaction_id"]
    flight_reservation_id = func_input["flight_reservation_id"]
    rentle_from = func_input["rentle_from"]
    rentle_to = func_input["rentle_to"]
    print(f"rent car at {rentle_from}")
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(rentle_from, date_format)
    end_date = datetime.strptime(rentle_to, date_format)
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        cap = store.get(date_str)
        if type(cap) is not int:
            cap = int(cap)
        current_date += timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(date_format)
        cap = store.get(date_str)
        if type(cap) is not int:
            cap = int(cap)
        store.put(date_str, str(cap - 1))
        current_date += timedelta(days=1)
    car_reservation_id = f"{transaction_id}_{rentle_from}_{rentle_to}"
    store.put(car_reservation_id, 'Pending')
    store.ret({'transaction_id':transaction_id, "car_reservation_id":car_reservation_id, 'flight_reservation_id': flight_reservation_id})