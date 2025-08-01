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
sys.path.append(str(ROOT_DIR))
import config

TEXT_SIZE_SMALL = 8
TEXT_SIZE_LARGE = 8 * 1024  # 8B / 8KB
DB_SIZE = 20
STOREGE_NODE_IP = config.STOREGE_NODE_IP
time.sleep(2)
couch_db = couchdb.Server(f'http://faasnap:faasnap@{STOREGE_NODE_IP}:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STOREGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')


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

table.meta.client.get_waiter('table_exists').wait(TableName='data')
startup_version = datetime(2000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')

db_keys = {'large':[], 'small':[]}
for i in range(DB_SIZE):
    small_key_name = f"key{i}small"
    large_key_name = f"key{i}large"
    db_keys['small'].append(small_key_name)
    db_keys['large'].append(large_key_name)
    small_text = generate_random_text(TEXT_SIZE_SMALL)
    large_text = generate_random_text(TEXT_SIZE_LARGE)
    table.put_item(
        Item={
            'key': small_key_name,
            'version': startup_version,
            'value': small_text
        }
    )
    table.put_item(
        Item={
            'key': large_key_name,
            'version': startup_version,
            'value': large_text
        }
    )
json.dump(db_keys, open(script_dir / "db_keys.json", 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
