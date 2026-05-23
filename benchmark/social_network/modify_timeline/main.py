import json

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    publish_post_id = func_input["publish_post_id"]
    timeline_key = f"{user_id}_timeline"
    timeline = json.loads(store.get(timeline_key))
    timeline["posts"].append(publish_post_id)
    timeline_length = len(timeline["posts"])
    store.put(timeline_key, json.dumps(timeline))
    store.ret({'timeline_length':timeline_length, 'publish_post_id':publish_post_id})