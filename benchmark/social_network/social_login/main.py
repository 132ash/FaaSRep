import json

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    password = func_input["password"]
    comment_post_id_1 = func_input["comment_post_id_1"]
    comment_post_id_2 = func_input["comment_post_id_2"]
    comment_post_id_3 = func_input["comment_post_id_3"]
    transaction_id = func_input["transaction_id"]
    comment_user_id = func_input["comment_user_id"]
    pwd_key = f"{user_id}_social_pwd"
    src_pwd = store.get(pwd_key)
    if src_pwd != password:
        store.abort_tx(f"Password for account {user_id} does not match. given: {password}, expected: {src_pwd}")
    store.ret({'user_id':user_id, "comment_post_id_1":comment_post_id_1, "comment_post_id_2":comment_post_id_2, "comment_post_id_3":comment_post_id_3, 'transaction_id': transaction_id, 'comment_user_id': comment_user_id})