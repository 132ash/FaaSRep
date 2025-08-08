import json
import datetime

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    comment_post_id_3 = func_input["comment_post_id_3"]
    post_content = json.loads(store.get(comment_post_id_3))
    comment_time = datetime.datetime.now().isoformat()
    comment_content = f"Comment by {user_id} on post {comment_post_id_3} at {comment_time}, Good!"
    comment = {
        "user_id": user_id,
        "content": comment_content,
        "time": comment_time
    }
    post_content["comments"].append(comment)
    store.put(comment_post_id_3, json.dumps(post_content))