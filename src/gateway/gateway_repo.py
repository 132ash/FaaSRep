from typing import Any, List
import couchdb
import redis
import boto3
import sys
import re

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

# interact with couchdb node

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        addrs = self.get_all_addrs('common')
        self.dynamo = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)
        
        self.redis = {
            host : redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB)
                for host in addrs
            }
 

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions']
            
    def get_end_function(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'end_function' in doc:
                return doc['end_function']   

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
            
    def get_tx_result(self, transaction_id):
        db = self.couch["results"]
        try:
            doc = db[transaction_id]
            return doc['result']
        except couchdb.http.ResourceNotFound:
            return None

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.couch[mode]
        for item in db.find({'selector': {'function_name': function_name}}):
            return item

    def create_request_doc(self, request_id: str) -> None:
        if request_id in self.couch['results']:
            doc = self.couch['results'][request_id]
            self.couch['results'].delete(doc)
        self.couch['results'][request_id] = {}

    def param_wrapper(self, transaction_id, mode, func ,key, is_dynamo):
        if is_dynamo:
            return f"{mode}:{func}:{key}"
        else:
            return f"{transaction_id}:{mode}:{func}:{key}" 
    
    def store_input(self, transaction_id, ip, input):
        shadow_table = self.dynamo.Table(f"{transaction_id}_shadow_table")
        for k, v in input.items():
            dynamo_key = self.param_wrapper(transaction_id, 'RET','GLOBAL', k, True)
            shadow_table.put_item(
                Item={
                    'key': dynamo_key,
                    'value': str(v)
                }
            )
        

    def get_result(self, request_id: str, workflow_name) -> Any:
        end_func = self.get_end_function(workflow_name + '_workflow_metadata')
        end_func_name = end_func['name']
        info = self.get_function_info(end_func['name'], workflow_name + '_function_info')
        ip = extract_ip(info['ip'])
        output = end_func['output']
        return self.fetch_result_from_shadow_table(request_id, end_func_name, output, ip)

    def fetch_result_from_shadow_table(self, transaction_id, func, output, redis_ip):
        keys = output.keys()
        result = {}
        shadow_table = self.dynamo.Table(f"{transaction_id}_shadow_table")
        for k in keys:
            dynamo_key = self.param_wrapper(transaction_id, 'RET',func, k, True)
            response = shadow_table.get_item(
                Key={
                    'key': dynamo_key
                }
            )
        item = response.get('Item')
        result[k] = int(item['value']) if output[k]["type"] == "int" else item['value'] 
        return result
    
    def release_lock(self, transaction_id, lock_set):
        """
        释放 lock_set 中每个 key 的锁，将其 lock 属性设置为 None。
        """
        data_db = self.dynamo.Table('data')

        for key in lock_set.keys():
            # 更新 lock 属性为 None
            data_db.update_item(
                Key={'key': key},
                UpdateExpression="SET #l = :none",
                ExpressionAttributeNames={
                    '#l': 'lock'
                },
                ConditionExpression="#l = :txid",  # 确保当前锁属于 transaction_id
                ExpressionAttributeValues={
                    ':txid': transaction_id,
                    ':none': None
                },
                ReturnValues="UPDATED_NEW"
            )

    def delete_latency(self, transaction_id):
        latency_db = self.couch['workflow_latency']
        for _id in self.couch['workflow_latency']:
            try:
                doc = self.couch['workflow_latency'][_id]
                if doc['transaction_id'] == transaction_id:
                    latency_db.delete(doc)
            except couchdb.http.ResourceNotFound:
                print(f"Latency document for transaction {transaction_id} not found, skipping deletion.")

    def create_shadow_table(self, transaction_id):
        table_name = f"{transaction_id}_shadow_table"
        existing_tables = self.dynamo.tables.all()
        if table_name not in [table.name for table in existing_tables]:
        # 创建表
            table = self.dynamo.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'key',
                        'KeyType': 'HASH'  # 主键
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'key',
                        'AttributeType': 'S'  # 字符串类型
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 100,
                    'WriteCapacityUnits': 100
                }
            )
            table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    
