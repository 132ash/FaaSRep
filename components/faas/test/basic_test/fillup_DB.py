import os
import boto3
import time

time.sleep(2)
dynamo_db  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')


table = dynamo_db.Table('data')

test_input_dir = 'test_input'
doc1_path = os.path.join(test_input_dir, 'doc1.txt')
doc2_path = os.path.join(test_input_dir, 'doc2.txt')

os.makedirs(test_input_dir, exist_ok=True)

# 生成4KB和3KB的文本文件
if not os.path.exists(doc1_path):
    with open(doc1_path, 'w') as f:
        f.write('A' * 4096)  # 4KB

if not os.path.exists(doc2_path):
    with open(doc2_path, 'w') as f:
        f.write('B' * 3072)  # 3KB

# 读取文件内容
with open(doc1_path, 'r') as f:
    doc1_content = f.read()

with open(doc2_path, 'r') as f:
    doc2_content = f.read()

# 将文件内容存储到 DynamoDB
table.put_item(
    Item={
        'key': 'doc1',
        'version': '0:0',
        'value': doc1_content
    }
)

table.put_item(
    Item={
        'key': 'doc2',
        'version': '0:0',
        'value': doc2_content
    }
)

response_doc1 = table.get_item(Key={'key': 'doc1'})
response_doc2 = table.get_item(Key={'key': 'doc2'})


if 'Item' in response_doc1 and response_doc1['Item']['value'] == doc1_content:
    print("doc1.txt stored successfully")
else:
    print("doc1.txt stored failed")

if 'Item' in response_doc2 and response_doc2['Item']['value'] == doc2_content:
    print("doc2.txt stored successfully")
else:
    print("doc2.txt stored failed")