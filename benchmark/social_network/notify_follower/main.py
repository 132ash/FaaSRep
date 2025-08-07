import json
import datetime
import random
import string

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    publish_post_id = func_input["publish_post_id"]
    followers_key = f"{user_id}_followers"
    followers = json.loads(store.get(followers_key))
    notify_num = 0
    for follower in followers:
        mailbox_key = f"{follower}_mailbox"
        new_post_ids = json.loads(store.get(mailbox_key))
        new_post_ids.append(publish_post_id)
        store.put(mailbox_key, json.dumps(new_post_ids))
        notify_num += 1
    store.ret({'notify_followers_num': notify_num})

