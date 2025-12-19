

def main():
    func_input = store.fetch_input()
    src_account = func_input["src_account"]
    password = func_input["password"]
    dst_account = func_input["dst_account"]
    amount = func_input["amount"]
    pwd_key = f"{src_account}_bank_pwd"
    src_pwd = store.get(pwd_key)
    # if src_pwd != password:
    #     store.abort_tx(f"Password for account {src_account} does not match. given: {password}, expected: {src_pwd}")
    store.ret({'src_account':src_account, "dst_account":dst_account, 'amount': amount})