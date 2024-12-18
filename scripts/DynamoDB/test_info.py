import boto3

# 创建dynamodb资源对象
dynamodb  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table('data')
# 读取并展示样例数据
response = table.get_item(
    Key={
        'key': 'test_value'
    }
)
item = response.get('Item')
print(f"Key: {item['key']}, Version: {item['version']}, Value: {item['value']}")
