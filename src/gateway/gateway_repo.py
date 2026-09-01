from typing import Any, List
import couchdb
import redis
import boto3
import sys
import re
import logging
from pathlib import Path

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import config
from src.storage_schema import ensure_shadow_table

couchdb_url = config.COUCHDB_URL
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

CACHE_ENABLED = config.CACHE_ENABLED

# interact with couchdb node

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        addrs = self.get_all_addrs('common')
        self.dynamo = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)
        if config.SYSTEM_MODE == 'BOKI_SN':
            # Boki still needs this legacy private RET/input transport even
            # though application writes use src/shadow_service instead.
            ensure_shadow_table(self.dynamo)
        
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

    def param_wrapper(self, transaction_id, mode, func, key, is_dynamo, term=0):
        if is_dynamo:
            return f"{mode}:{term}:{func}:{key}"
        else:
            return f"{transaction_id}:{term}:{mode}:{func}:{key}"
    
    def store_input(self, transaction_id, ip, input, term=0):
        if CACHE_ENABLED:
            for k, v in input.items():
                redis_key = self.param_wrapper(transaction_id, 'RET', 'GLOBAL', k, False, term)
                self.redis[extract_ip(ip)][redis_key] = v
        else:
            shadow_table = self.dynamo.Table("shadow_table")
            for k, v in input.items():
                dynamo_key = self.param_wrapper(transaction_id, 'RET', 'GLOBAL', k, True, term)
                shadow_table.put_item(
                    Item={
                        'txid': transaction_id, # 分区键
                        'key': dynamo_key,      # 排序键
                        'value': str(v)
                    }
                )

    def get_result(self, request_id: str, workflow_name, term=0) -> Any:
        end_func = self.get_end_function(workflow_name + '_workflow_metadata')
        end_func_name = end_func['name']
        info = self.get_function_info(end_func['name'], workflow_name + '_function_info')
        ip = extract_ip(info['ip'])
        output = end_func['output']
        return self.fetch_result_from_shadow_table(request_id, end_func_name, output, ip, term)

    def fetch_result_from_shadow_table(self, transaction_id, func, output, redis_ip, term=0):
        keys = output.keys()
        result = {}
        if CACHE_ENABLED:   
            for k in keys:
                redis_key = self.param_wrapper(transaction_id, 'RET', func, k, False, term)
                result[k] = int(self.redis[redis_ip][redis_key].decode('utf-8')) if output[k]["type"] == "int" else self.redis[redis_ip][redis_key].decode('utf-8')
        else:
            shadow_table = self.dynamo.Table("shadow_table")
            for k in keys:
                dynamo_key = self.param_wrapper(transaction_id, 'RET', func, k, True, term)
                response = shadow_table.get_item(
                    Key={
                        'txid': transaction_id, # 分区键
                        'key': dynamo_key       # 排序键
                    }
                )
                item = response.get('Item')
                ##log_message(f"Fetched item for key {dynamo_key}: {item}")
                result[k] = int(item['value']) if output[k]["type"] == "int" else item['value'] 
        return result
    
   
    def clear_db(self, transaction_id):
        db = self.couch['results']
        if transaction_id in db:
            doc = db[transaction_id]
            db.delete(doc)
        # 从全局表中批量删除该事务的所有条目
        shadow_table = self.dynamo.Table('shadow_table')
        self._batch_delete_from_partition(shadow_table, transaction_id)
        


    def _batch_delete_from_partition(self, table, txid):
        """一个辅助函数，用于高效删除一个分区下的所有条目。"""
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
            ##log_message(f"Batch deleted all items for txid '{txid}' from table '{table.name}'.")
