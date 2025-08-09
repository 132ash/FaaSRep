import couchdb
import boto3
from datetime import datetime, timedelta
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

BANKING_ACCOUNTS = config.BANKING_ACCOUNTS
BANKING_ORIGINAL_BALANCE = config.BANKING_ORIGINAL_BALANCE

STOREGE_NODE_IP = config.STOREGE_NODE_IP

couch_db = couchdb.Server(f'http://faasnap:faasnap@{STOREGE_NODE_IP}:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STOREGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
startup_version = datetime(2025, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')

def generate_random_text(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


def create_banking_system_dataset():
    table = dynamo_db.Table('data')
    table.meta.client.get_waiter('table_exists').wait(TableName='data')
    password_per_account = {}
    # generate flight capacity data
    for i in range(BANKING_ACCOUNTS):
        account_id = f"account_{i}"
        pwd_key = f"{account_id}_bank_pwd"
        balance_key = f"{account_id}_balance"
        random_pwd = generate_random_text(10)
        table.put_item(
            Item={
                'key': pwd_key,
                'version': startup_version,
                'value': random_pwd
            }
        )
        table.put_item(
            Item={
                'key': balance_key,
                'version': startup_version,
                'value': BANKING_ORIGINAL_BALANCE
            }
        )
        password_per_account[account_id] = random_pwd
    json.dump(password_per_account, open(ROOT_DIR /"experiment"/"actual_apps"/ "banking_pwd.json", 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print(f"Generated {BANKING_ACCOUNTS} banking accounts with original balance {BANKING_ORIGINAL_BALANCE}")

if __name__ == "__main__":
    create_banking_system_dataset()
    print("Banking system dataset created successfully.")