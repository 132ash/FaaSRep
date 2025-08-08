
def main():
    func_input = store.fetch_input()
    src_account = func_input["src_account"]
    dst_account = func_input["dst_account"]
    amount = func_input["amount"]
    src_balance_key = f"{src_account}_balance"
    src_balance = store.get(src_balance_key)
    if type(src_balance) is not int:
        src_balance = int(src_balance)
    if src_balance < amount:
        store.abort_tx(f"Insufficient funds in account {src_account}. Current balance: {src_balance}, requested amount: {amount}")
    src_balance_new = src_balance - amount
    store.put(src_balance_key, str(src_balance_new))
    store.ret({"dst_account":dst_account, 'amount': amount, 'src_balance': src_balance_new})