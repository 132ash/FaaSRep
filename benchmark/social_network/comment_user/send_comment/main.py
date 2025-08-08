import json
import datetime

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    new_post_ids = json.loads(func_input["new_post_ids"])
    comment_post_id = func_input["comment_post_id"]
    new_post_ids.append(comment_post_id)
    comment_num = 0
    for post_id in new_post_ids:
        post_content = json.loads(store.get(post_id))
        comment_content = f"Comment by {user_id} on post {post_id}, Good!"
        comment_time = datetime.datetime.now().isoformat()
        comment = {
            "user_id": user_id,
            "content": comment_content,
            "time": comment_time
        }
        post_content["comments"].append(comment)
        comment_num += 1
    store.put(post_id, json.dumps(post_content))
    store.ret({'comment_num':comment_num})