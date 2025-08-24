import boto3
from datetime import date, timedelta

def check_data_db_values():
    """
    连接到 DynamoDB 并检查 data 表中指定范围的键值。
    """
    try:
        # 1. 创建 DynamoDB 资源对象
        dynamodb = boto3.resource(
            'dynamodb', 
            endpoint_url='http://10.2.29.142:4567', 
            aws_secret_access_key='FAASNAPDYNAMODBKEY', 
            aws_access_key_id='FAASNAPDYNAMODB', 
            region_name='us-west-2'
        )
        
        data_table = dynamodb.Table('data')
        print(f"成功连接到 DynamoDB 并获取表 'data' 的引用。")

    except Exception as e:
        print(f"连接到 DynamoDB 失败: {e}")
        return

    # 2. 生成需要查询的键列表
    keys_to_fetch = []
    
    # 生成 flight_1 到 flight_50
    for i in range(1, 51):
        keys_to_fetch.append({'key': f'flight_{i}'})
        
    # 生成 2025-07-01 到 2025-07-31
    start_date = date(2025, 7, 1)
    end_date = date(2025, 7, 31)
    delta = timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        keys_to_fetch.append({'key': current_date.strftime("%Y-%m-%d")})
        current_date += delta

    # 3. 使用 batch_get_item 批量获取数据
    try:
        print("\n正在批量获取数据...")
        response = dynamodb.batch_get_item(
            RequestItems={
                'data': {
                    'Keys': keys_to_fetch,
                    'ConsistentRead': True  # 使用强一致性读以获取最新数据
                }
            }
        )
        
        results = response.get('Responses', {}).get('data', [])
        
        # 将结果存入字典以便快速查找
        results_map = {item['key']: item for item in results}
        print(f"成功获取 {len(results)} 条记录。")

        # 4. 打印结果
        print("\n--- Flight 数据 (flight_1 到 flight_50) ---")
        for i in range(1, 51):
            key = f'flight_{i}'
            item = results_map.get(key)
            if item:
                print(f"  Key: {key}, Value: {item.get('value')}, Version: {item.get('version')}")
            else:
                print(f"  Key: {key} - 未找到")

        print("\n--- Date 数据 (2025-07-01 到 2025-07-31) ---")
        current_date = start_date
        while current_date <= end_date:
            key = current_date.strftime("%Y-%m-%d")
            item = results_map.get(key)
            if item:
                print(f"  Key: {key}, Value: {item.get('value')}, Version: {item.get('version')}")
            else:
                print(f"  Key: {key} - 未找到")
            current_date += delta

        # 检查是否有未处理的键（在单次请求超过100个或数据量大时可能发生）
        if 'UnprocessedKeys' in response and response['UnprocessedKeys']:
            print("\n警告: 有部分键未被处理，可能需要重试:")
            print(response['UnprocessedKeys'])

    except Exception as e:
        print(f"\n在获取数据时发生错误: {e}")

if __name__ == '__main__':
    check_data_db_values()
