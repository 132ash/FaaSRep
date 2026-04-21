import couchdb
import boto3
from datetime import datetime
import random
import json
import string
import sys
from pathlib import Path

def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

def generate_random_text(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

script_dir = Path(__file__).parent
ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR / 'config'))
sys.path.append(str(script_dir.parent))
import config

SOCIAL_NETWORK_USERS = config.SOCIAL_NETWORK_USERS
STARTUP_POSTS = config.STARTUP_POSTS
STORAGE_NODE_IP = config.STORAGE_NODE_IP
couch_db = couchdb.Server(config.COUCHDB_URL)
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STORAGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
startup_version = datetime(2025, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')



def create_social_network_dataset():
    table = dynamo_db.Table('data')
    table.meta.client.get_waiter('table_exists').wait(TableName='data')
    all_users = [f"user_{i}" for i in range(SOCIAL_NETWORK_USERS)]
    password_per_account= {}
    original_post_ids = []
    
    # generate user data
    for i in range(SOCIAL_NETWORK_USERS):
        user_id = f"user_{i}"
        pwd_key = f"{user_id}_social_pwd"
        random_pwd = generate_random_text(10)
        table.put_item(
            Item={
                'key': pwd_key,
                'version': startup_version,
                'value': random_pwd
            }
        )
        password_per_account[user_id] = random_pwd

        startup_post_ids = [f"{user_id}_startup_post_{j}" for j in range(STARTUP_POSTS)]
        original_post_ids.extend(startup_post_ids)
        for post_id in startup_post_ids:
            post_content = json.dumps({'text':generate_random_text(100), "publisher":user_id, "publish_time":datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'comments':[]})
            table.put_item(
                Item={
                    'key': post_id,
                    'version': startup_version,
                    'value': post_content
                }
            )
        table.put_item(
            Item={
                'key': f"{user_id}_timeline",
                'version': startup_version,
                'value': json.dumps({'comment_num':0, 'posts': startup_post_ids})
            }
        )
    json.dump(password_per_account, open(ROOT_DIR /"experiment"/"actual_apps"/ "social_pwd.json", 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    json.dump(original_post_ids, open(ROOT_DIR /"experiment"/"actual_apps"/ "social_posts.json", 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print(f"Generated {SOCIAL_NETWORK_USERS} user records with {STARTUP_POSTS} startup posts each.")

if __name__ == "__main__":
    create_social_network_dataset()
    print("Social network dataset created successfully.")