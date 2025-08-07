

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    mailbox_key = f"{user_id}_mailbox"
    new_post_ids = store.get(mailbox_key)
    store.ret({'new_post_ids':new_post_ids})