
def main():
    func_input = store.fetch_input()
    dst_account = func_input["dst_account"]
    src_balance = func_input["src_balance"]
    amount = func_input["amount"]
    dst_balance_key = f"{dst_account}_balance"
    dst_balance = store.get(dst_balance_key)
    if type(dst_balance) is not int:
        dst_balance = int(dst_balance)
    dst_balance_new = dst_balance + amount
    store.put(dst_balance_key, str(dst_balance_new))
    store.ret({'dst_balance': dst_balance_new, 'src_balance': src_balance})
