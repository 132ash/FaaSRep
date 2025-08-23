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
        # 使用全局的 shadow_table
        shadow_table = self.dynamo.Table("shadow_table")
        for k, v in input.items():
            dynamo_key = self.param_wrapper(transaction_id, 'RET','GLOBAL', k, True)
            shadow_table.put_item(
                Item={
                    'txid': transaction_id, # 分区键
                    'key': dynamo_key,      # 排序键
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
        # 使用全局的 shadow_table
        shadow_table = self.dynamo.Table("shadow_table")
        #log_message(f"Fetching result for transaction {transaction_id} from global shadow_table")
        for k in keys:
            dynamo_key = self.param_wrapper(transaction_id, 'RET',func, k, True)
            response = shadow_table.get_item(
                Key={
                    'txid': transaction_id, # 分区键
                    'key': dynamo_key       # 排序键
                }
            )
            item = response.get('Item')
            #log_message(f"Fetched item for key {dynamo_key}: {item}")
            result[k] = int(item['value']) if output[k]["type"] == "int" else item['value'] 
        return result
    
    def _batch_delete_from_partition(self, table, txid):
        """一个辅助函数，用于高效删除一个分区下的所有条目。"""
        try:
            with table.batch_writer() as batch:
                # 1. 查询分区下的所有 key
                response = table.query(
                    KeyConditionExpression='txid = :txid',
                    ExpressionAttributeValues={':txid': txid}
                )
                keys_to_delete = response.get('Items', [])
                # 处理分页，以防一个分区下的条目超过1MB
                while 'LastEvaluatedKey' in response:
                    response = table.query(
                        KeyConditionExpression='txid = :txid',
                        ExpressionAttributeValues={':txid': txid},
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    keys_to_delete.extend(response.get('Items', []))
                
                # 2. 批量删除
                for item in keys_to_delete:
                    batch.delete_item(Key={'txid': item['txid'], 'key': item['key']})
            #log_message(f"Batch deleted all items for txid '{txid}' from table '{table.name}'.")
        except Exception as e:
            log_message(f"Error during batch delete for txid '{txid}' from table '{table.name}': {e}")

    def delete_latency(self, transaction_id):
        latency_db = self.couch['workflow_latency']
        for _id in self.couch['workflow_latency']:
            try:
                doc = self.couch['workflow_latency'][_id]
                if doc['transaction_id'] == transaction_id:
                    latency_db.delete(doc)
            except couchdb.http.ResourceNotFound:
                log_message(f"Latency document for transaction {transaction_id} not found, skipping deletion.")

    def reset_and_release_locks_for_retry(self, transaction_id):
        # 使用全局的 lock_shadow_table
        lock_table = self.dynamo.Table('lock_shadow_table')
        data_db = self.dynamo.Table('data')
        
        # 初始化 term
        lock_table.update_item(
            Key={'txid': transaction_id, 'key': '_term_'},
            UpdateExpression="SET #v = if_not_exists(#v, :start) + :inc",
            ExpressionAttributeNames={'#v': 'value'},
            ExpressionAttributeValues={':inc': 1, ':start': 0}
        )
        
        # 查询属于该事务的所有锁
        response = lock_table.query(
            KeyConditionExpression="txid = :txid",
            ExpressionAttributeValues={":txid": transaction_id}
        )
        all_items = response.get('Items', [])
        old_locks = [item for item in all_items if item.get('key') != '_term_']

        for lock_item in old_locks:
            key_to_release = lock_item['key']
            data_db.update_item(
                Key={'key': key_to_release},
                UpdateExpression="REMOVE #l",
                ConditionExpression="#l.txid = :txid",
                ExpressionAttributeNames={'#l': 'lock'},
                ExpressionAttributeValues={':txid': transaction_id}
            )
        with lock_table.batch_writer() as batch:
            for item in old_locks:
                batch.delete_item(Key={'txid': item['txid'], 'key': item['key']})

    def release_all_locks(self, transaction_id):
        lock_table = self.dynamo.Table('lock_shadow_table')
        data_db = self.dynamo.Table('data')
        response = lock_table.query(
            KeyConditionExpression="txid = :txid",
            ExpressionAttributeValues={":txid": transaction_id}
        )

        all_items = response.get('Items', [])
        locks_to_release = [item for item in all_items if item.get('key') != '_term_']

        for lock_item in locks_to_release:
            key_to_release = lock_item['key']
            data_db.update_item(
                Key={'key': key_to_release},
                UpdateExpression="REMOVE #l",
                ConditionExpression="#l.txid = :txid",
                ExpressionAttributeNames={'#l': 'lock'},
                ExpressionAttributeValues={':txid': transaction_id}
            )

    def clear_db(self, transaction_id):
        db = self.couch['results']
        if transaction_id in db:
            doc = db[transaction_id]
            db.delete(doc)
        
        # 从全局表中批量删除该事务的所有条目
        shadow_table = self.dynamo.Table('shadow_table')
        self._batch_delete_from_partition(shadow_table, transaction_id)
        
        lock_shadow_table = self.dynamo.Table('lock_shadow_table')
        self._batch_delete_from_partition(lock_shadow_table, transaction_id)
