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

FLIGHT_IDS = config.FLIGHT_IDS

FLIGHT_CAPACITY = config.FLIGHT_CAPACITY
RENTAL_START = config.RENTAL_START
RENTAL_END = config.RENTAL_END
CAR_NUM = config.CAR_NUM
STORAGE_NODE_IP = config.STORAGE_NODE_IP
DATE_FORMAT = config.DATE_FORMAT
couch_db = couchdb.Server(config.COUCHDB_URL)
dynamo_db  = boto3.resource('dynamodb', endpoint_url=f'http://{STORAGE_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
startup_version = datetime(2025, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')

def create_travel_reservation_dataset():
    table = dynamo_db.Table('data')
    table.meta.client.get_waiter('table_exists').wait(TableName='data')

    # generate flight capacity data
    for i in range(FLIGHT_IDS):
        flight_id = f"flight_{i}"
        table.put_item(
            Item={
                'key': flight_id,
                'version': startup_version,
                'value': FLIGHT_CAPACITY
            }
        )
    print(f"Generated {FLIGHT_IDS} flight records with capacity {FLIGHT_CAPACITY}")
    start_date = datetime.strptime(RENTAL_START, DATE_FORMAT)
    end_date = datetime.strptime(RENTAL_END, DATE_FORMAT)
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime(DATE_FORMAT)
        table.put_item(
            Item={
                'key': date_str,
                'version': startup_version,
                'value': CAR_NUM
            }
        )
        current_date += timedelta(days=1)
    print(f"Generated {CAR_NUM} car records for each date from {RENTAL_START} to {RENTAL_END}")

if __name__ == "__main__":
    create_travel_reservation_dataset()
    print("Travel reservation dataset created successfully.")