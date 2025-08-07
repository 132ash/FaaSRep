import json

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    publish_post_id = func_input["publish_post_id"]
    comment_num = func_input["comment_num"]
    notify_followers_num = func_input["notify_followers_num"]
    timeline_key = f"{user_id}_timeline"
    timeline = json.loads(store.get(timeline_key))
    all_comment_num = timeline["comment_num"]
    all_comment_num += comment_num
    timeline["comment_num"] = all_comment_num
    timeline["posts"].append(publish_post_id)
    timeline_length = len(timeline["posts"])
    store.ret({'timeline_length':timeline_length, 'publish_post_id':publish_post_id, 'comment_num': all_comment_num, 'notify_followers_num': notify_followers_num})