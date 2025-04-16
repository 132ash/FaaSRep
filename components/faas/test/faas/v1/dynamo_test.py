import boto3

# 创建dynamodb资源对象
dynamodb  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# transaction_id = '532dcb5d-2559-4075-a3a5-c90fef1a033f'
# table_name = f"{transaction_id}_shadow_table"
table_name = "data"
# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table(table_name)
# 读取并展示样例数据
# 扫描表并打印所有键值对
response = table.scan()
items = response.get('Items', [])
for item in items:
    print(type(item['value']))
    print(int(item['value']))
    print(f"Key: {item['key']}, Value: {item['value']}, lock: {item['lock']}")
