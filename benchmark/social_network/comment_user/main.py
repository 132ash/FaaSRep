import json
import datetime

def main():
    func_input = store.fetch_input()
    user_id = func_input["user_id"]
    comment_user_id = func_input["comment_user_id"]
    target_user_timeline_key = f"{comment_user_id}_timeline"
    target_timeline = json.loads(store.get(target_user_timeline_key))
    newest_post_id = target_timeline["posts"][0]
    post_content = json.loads(store.get(newest_post_id))
    comment_time = datetime.datetime.now().isoformat()
    comment_content = f"Comment by {user_id} on post {newest_post_id} for user {comment_user_id} at {comment_time}, Good!"
    post_content["comments"].append({
        "user_id": user_id,
        "content": comment_content,
        "time": comment_time
    })
    store.put(newest_post_id, json.dumps(post_content))
