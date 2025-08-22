import boto3

# 创建dynamodb资源对象
dynamodb  = boto3.resource('dynamodb', endpoint_url='http://10.2.29.142:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# 检查所有以 lock_shadow_table 结尾的表
for table_info in dynamodb.tables.all():
    table_name = table_info.name
    if table_name.endswith('shadow_table'):
        print(f"\n检查表: {table_name}")
        table = dynamodb.Table(table_name)
        response = table.scan()
        items = response.get('Items', [])
        for item in items:
            # print(f"Item: {item}")
            for k, v in item.items():
                if v is None:
                    print(f"  字段: {k}, 类型: NoneType, 值: None")
                # print(f"  字段: {k}, 类型: {type(v).__name__}, 值: {v}")