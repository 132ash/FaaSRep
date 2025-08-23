from typing import Any, List
import couchdb
import redis
import boto3
import sys
import re
import os
from botocore.exceptions import ClientError
import logging
log_file = '../../logging/gateway_repo.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

def setup_logger():
    logger = logging.getLogger('gateway_repo')
    logger.setLevel(logging.INFO)
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# 全局logger实例
logger = setup_logger()

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()

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
        log_message(f"Fetching result for transaction {transaction_id} from shadow table {transaction_id}_shadow_table")
        for k in keys:
            dynamo_key = self.param_wrapper(transaction_id, 'RET',func, k, True)
            response = shadow_table.get_item(
                Key={
                    'key': dynamo_key
                }
            )
            item = response.get('Item')
            log_message(f"Fetched item for key {dynamo_key}: {item}")
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
                log_message(f"Latency document for transaction {transaction_id} not found, skipping deletion.")

    def create_shadow_table(self, transaction_id):
        # --- 任务 1: 同时创建 data_shadow_table 和 lock_shadow_table ---
        # 创建数据影子表
        table_name = f"{transaction_id}_shadow_table"
        if not self.table_exists(table_name):
            self.dynamo.create_table(
                TableName=table_name,
                KeySchema=[{'AttributeName': 'key', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'key', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 100, 'WriteCapacityUnits': 100}
            )
            self.dynamo.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        
        # 创建锁影子表
        lock_table_name = f"{transaction_id}_lock_shadow_table"
        if not self.table_exists(lock_table_name):
            lock_table = self.dynamo.create_table(
                TableName=lock_table_name,
                KeySchema=[{'AttributeName': 'key', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'key', 'AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 100, 'WriteCapacityUnits': 100}
            )
            lock_table.wait_until_exists()
            # 初始化 term 为 0
            lock_table.put_item(Item={'key': '_term_', 'value': 0})

    def table_exists(self, table_name):
        try:
            self.dynamo.meta.client.describe_table(TableName=table_name)
            return True
        except self.dynamo.meta.client.exceptions.ResourceNotFoundException:
            return False

    def reset_and_release_locks_for_retry(self, transaction_id):
        lock_table_name = f"{transaction_id}_lock_shadow_table"
        lock_table = self.dynamo.Table(lock_table_name)
        data_db = self.dynamo.Table('data')
        lock_table.update_item(Key={'key': '_term_'}, UpdateExpression="SET #v = #v + :inc",
                               ExpressionAttributeNames={'#v': 'value'},
                               ExpressionAttributeValues={':inc': 1})
        response = lock_table.scan(FilterExpression="attribute_exists(#k) AND #k <> :state_key",
                                   ExpressionAttributeNames={"#k": "key"},
                                   ExpressionAttributeValues={":state_key": "_term_"})
        old_locks = response.get('Items', [])
        for lock_item in old_locks:
            key_to_release = lock_item['key']
            try:
                data_db.update_item(
                    Key={'key': key_to_release},
                    UpdateExpression="REMOVE #l",
                    ConditionExpression="#l.txid = :txid",
                    ExpressionAttributeNames={'#l': 'lock'},
                    ExpressionAttributeValues={':txid': transaction_id}
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    log_message(f"Error releasing global lock for {key_to_release}: {e}")
            lock_table.delete_item(Key={'key': key_to_release})

    def release_all_locks(self, transaction_id):
        """任务 3: 释放一个成功事务持有的所有锁。"""
        lock_table_name = f"{transaction_id}_lock_shadow_table"
        lock_table = self.dynamo.Table(lock_table_name)
        data_db = self.dynamo.Table('data')

        response = lock_table.scan(FilterExpression="attribute_exists(#k) AND #k <> :state_key",
                                   ExpressionAttributeNames={"#k": "key"},
                                   ExpressionAttributeValues={":state_key": "_term_"})
        
        for lock_item in response.get('Items', []):
            key_to_release = lock_item['key']
            try:
                data_db.update_item(
                    Key={'key': key_to_release},
                    UpdateExpression="REMOVE #l",
                    ConditionExpression="#l.txid = :txid",
                    ExpressionAttributeNames={'#l': 'lock'},
                    ExpressionAttributeValues={':txid': transaction_id}
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    log_message(f"Error releasing global lock for {key_to_release} on commit: {e}")


    def delete_shadow_table(self, transaction_id=None):
        if transaction_id is None:
            # 删除所有以 "_shadow_table" 结尾的表
            for table in self.dynamo.tables.all():
                if table.name.endswith('_shadow_table'):
                    t = self.dynamo.Table(table.name)
                    t.delete()
                    t.meta.client.get_waiter('table_not_exists').wait(TableName=table.name)
        else:
            table_name = f"{transaction_id}_shadow_table"
            existing_tables = self.dynamo.tables.all()
            if table_name in [table.name for table in existing_tables]:
                table = self.dynamo.Table(table_name)
                table.delete()
                table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)

    def clear_db(self, transaction_id=''):
        if transaction_id:
            db = self.couch['results']
            if transaction_id in db:
                doc = db[transaction_id]
                db.delete(doc)

            table_name = f"{transaction_id}_shadow_table"
            if self.table_exists(table_name):
                self.dynamo.Table(table_name).delete()

            lock_table_name = f"{transaction_id}_lock_shadow_table"
            if self.table_exists(lock_table_name):
                self.dynamo.Table(lock_table_name).delete()
        else:
            for table in self.dynamo.tables.all():
                if table.name.endswith('_shadow_table'):
                    t = self.dynamo.Table(table.name)
                    t.delete()
                    t.meta.client.get_waiter('table_not_exists').wait(TableName=table.name)
            log_message("Cleared all shadow tables.")