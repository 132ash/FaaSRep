import couchdb
import boto3
import sys
import random
import yaml
import string
from pathlib import Path
TEXT_SIZE = 4 * 1024  # 1MB / 4KB

def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

script_dir = Path(__file__).parent
ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

import config.config as config

STORAGE_NODE_IP = config.STORAGE_NODE_IP

couch_db = couchdb.Server(f'http://faasnap:faasnap@{STORAGE_NODE_IP}:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STORAGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

for d in ["workflow_latency", "common", "results", "log"]:
    if d in couch_db:
        del couch_db[d]
    couch_db.create(d)

try:
    table = dynamo_db.Table('data')
    table.delete()
    table.meta.client.get_waiter('table_not_exists').wait(TableName='data')
except:
    pass

project_root = Path(__file__).parent
while project_root != project_root.parent:
    if (project_root / "README.md").exists():
        break
    project_root = project_root.parent
CONFIG_DIR = project_root / 'config'
node_info = yaml.load(open(f'{CONFIG_DIR}/worker_info.yaml'), Loader=yaml.FullLoader)["nodes"]
couch_db['common'].save({'addrs': list(node_info)})

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
print("Table 'data' created successfully.")

# 生成 4KB 的随机文本
print(f"Generating {TEXT_SIZE // 1024}KB random text for 'test_value'...")
random_text = ''.join(random.choices(string.ascii_letters + string.digits, k=TEXT_SIZE))

# 将生成的随机文本存入 data 表
table.put_item(
    Item={
        'key': 'test_value',
        'value': random_text
    }
)




