import json

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    password = func_input["password"]
    comment_post_id = func_input["comment_post_id"]
    transaction_id = func_input["transaction_id"]
    pwd_key = f"{user_id}_social_pwd"
    print(f"Checking password for user {user_id} with transaction ID {transaction_id}, pwd_key: {pwd_key}", flush=True)
    src_pwd = store.get(pwd_key)
    if src_pwd != password:
        store.abort_tx(f"Password for account {user_id} does not match. given: {password}, expected: {src_pwd}")
    store.ret({'user_id':user_id, "comment_post_id":comment_post_id, 'transaction_id': transaction_id})