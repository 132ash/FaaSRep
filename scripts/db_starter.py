import couchdb
import boto3
import time
from datetime import datetime
import random
import string

TEXT_SIZE = 4 * 1024  # 1MB / 4KB

couch_db = couchdb.Server('http://faasnap:faasnap@10.2.27.24:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url='http://10.2.27.24:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')


for d in ["workflow_latency", "common", "results", "log"]:
    if d not in couch_db:
        couch_db.create(d)

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




