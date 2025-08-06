def main():
    func_input = store.fetch_input()
    transaction_id = func_input["transaction_id"]
    flight_id = func_input["flight_id"]
    rentle_from = func_input["rentle_from"]
    rentle_to = func_input["rentle_to"]
    flight_cap = store.get(flight_id)
    if type(flight_cap) is not int:
        flight_cap = int(flight_cap)
    if flight_cap == 0:
        store.abort_tx(f"Flight {flight_id} is full.")
    store.put(flight_id, str(flight_cap - 1))
    flight_reservation_id = f"{transaction_id}_{flight_id}"
    store.put(flight_reservation_id, 'Pending')
    store.ret({'transaction_id':transaction_id, "flight_reservation_id":flight_reservation_id, 'rentle_from': rentle_from, 'rentle_to': rentle_to})