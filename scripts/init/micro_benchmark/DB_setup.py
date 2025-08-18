import couchdb
import boto3
import time
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
import config

DB_SIZE = config.DB_SIZE
DATA_ITEM_SIZE = config.DATA_ITEM_SIZE  
STORAGE_NODE_IP = config.STORAGE_NODE_IP
couch_db = couchdb.Server(f'http://faasnap:faasnap@{STORAGE_NODE_IP}:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STORAGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

def flush_data_db():
    try:
        table = dynamo_db.Table('data')
        table.delete()
        table.meta.client.get_waiter('table_not_exists').wait(TableName='data')
    except:
        pass
    table = dynamo_db.create_table(
        TableName='data',
        KeySchema=[
            {
                'AttributeName': 'key',
                'KeyType': 'HASH'  # 主键
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'key',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 100
        }
    )

def create_microbenchmark_dataset(flush=False):
    if flush:
        flush_data_db()
    table = dynamo_db.Table('data')
    table.meta.client.get_waiter('table_exists').wait(TableName='data')
    startup_version = datetime(2025, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')

    db_keys = []
    for i in range(DB_SIZE):
        key_name = f"key{i}"
        text = generate_random_text(DATA_ITEM_SIZE)
        table.put_item(
            Item={
                'key': key_name,
                'version': startup_version,
                'value': text
            }
        )
        db_keys.append(key_name)
    json.dump(db_keys, open(ROOT_DIR /"experiment"/"microbenchmark"/ "db_keys.json", 'w', encoding='utf-8'), ensure_ascii=False, indent=2)

if __name__ == "__main__":
    flush = sys.argv[1].lower() == 'flush' if len(sys.argv) > 1 else False
    create_microbenchmark_dataset(flush=flush)
    print("Microbenchmark dataset created successfully. flush db:{}".format(flush))
