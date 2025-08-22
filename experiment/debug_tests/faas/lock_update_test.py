import boto3
from botocore.exceptions import ClientError

# 创建 DynamoDB 资源和 Client
dynamodb = boto3.resource('dynamodb', endpoint_url='http://10.2.29.142:4567',
                          aws_secret_access_key='FAASNAPDYNAMODBKEY',
                          aws_access_key_id='FAASNAPDYNAMODB',
                          region_name='us-west-2')
client = dynamodb.meta.client

table_name = 'test_lock_shadow_table'

# 1. 创建表
try:
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'key', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'key', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    table.wait_until_exists()
    print(f"表 {table_name} 创建成功")
except client.exceptions.ResourceInUseException:
    table = dynamodb.Table(table_name)
    print(f"表 {table_name} 已存在")

# 2. 初始化 _term_ 项
table.put_item(Item={'key': '_term_', 'value': 0})
print("初始化 _term_ 项完成")

# 3. 原子化更新锁记录（模拟 beldi_store 的 transact_write_items）
lock_key = 'user_1_social_pwd'
term = 0

try:
    response = client.transact_write_items(
        TransactItems=[
            {
                'ConditionCheck': {
                    'TableName': table_name,
                    'Key': {'key': {'S': '_term_'}},
                    'ConditionExpression': '#v = :term',
                    'ExpressionAttributeNames': {'#v': 'value'},
                    'ExpressionAttributeValues': {':term': {'N': str(term)}}
                }
            },
            {
                'Put': {
                    'TableName': table_name,
                    'Item': {'key': {'S': lock_key}, 'value': {'N': '1'}}
                }
            }
        ]
    )
    print(f"原子化加锁成功: {lock_key}")
except ClientError as e:
    print(f"加锁失败: {e}")