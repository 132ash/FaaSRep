import json
import datetime
import random
import string

def generate_random_text(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    transaction_id = func_input["transaction_id"]
    publish_post_id = f"{user_id}_{transaction_id}_post"
    post_text = generate_random_text(100)

    store.put(publish_post_id, json.dumps({
        "publisher": user_id,
        "text": post_text,
        "publish_time": datetime.datetime.now().isoformat(),
        "comments": []
    }))
    store.ret({'publish_post_id':publish_post_id})