import couchdb
import boto3
import time

time.sleep(2)
couch_db = couchdb.Server('http://faasnap:faasnap@127.0.0.1:5984')
dynamo_db  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')


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

table.meta.client.get_waiter('table_exists').wait(TableName='data')

table.put_item(
    Item={
        'key': 'test_value',
        'version': '0:0',
        'value': '1'
    }
)

