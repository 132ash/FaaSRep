from gevent import monkey
monkey.patch_all()
from typing import Dict, List, Any
import couchdb
import redis
import boto3
from datetime import datetime
import sys
import json

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

class DynamoDBClient:
    def __init__(self, endpoint_url, aws_secret_access_key, aws_access_key_id, region_name):
        self.client = boto3.resource('dynamodb', endpoint_url=endpoint_url, aws_secret_access_key=aws_secret_access_key, aws_access_key_id=aws_access_key_id, region_name=region_name)
        self.table = self.client.Table('data')

    def get_all_data_from_db(self):
        response = self.table.scan()
        return response['Items']

    def get_data_from_db(self, key):
        # 从dynamodb中获取数据
        response = self.table.get_item(
            Key={
                'key': key
            }
        )
        item = response.get('Item')
        if item:
            return item['value']
        else:
            return None

    def store_data_to_db(self, key, value):
        self.table.put_item(
            Item={
                'key': key,
                'value': value
            }
        )

class Repository:
    def __init__(self):
        self.cache_redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.CACHE_DB)
        self.data_db = DynamoDBClient(dynamodb_url, dynamodb_access_key, dynamodb_key_id, dynamodb_area)
        self.couch = couchdb.Server(couchdb_url)
        self.shadowtable_redis_all_addr:Dict[str, redis.StrictRedis] =  {
                    host:redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB, decode_responses=True)
                    for host in self.get_all_addrs('common')
                    }

    def shadowtable_init(self, ip):
        self.ip = ip
        
    # get all function_name for every node seems to solve the problem of KeyError Exception in manager.py, line 103
    def get_current_node_functions(self, ip: str, mode: str) -> List[str]:
        db = self.couch[mode]
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions'] 
            
    def get_end_function(self, db_name) -> str:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'end_function' in doc:
                return doc['end_function']['name']
            

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.couch[mode]
        for item in db.find({'selector': {'function_name': function_name}}):
            return item
    
    def clear_mem(self, transaction_id=""):
        if transaction_id:
            print(f"clearing shadow table for {transaction_id}")
            keys = self.shadowtable_redis_all_addr[self.ip].keys(f"{transaction_id}:*")
            if keys:
                pipe = self.shadowtable_redis_all_addr[self.ip].pipeline()
                for key in keys:
                    pipe.delete(key)
                pipe.execute()
        else:
            print("clearing all shadow tables and cache")
            self.shadowtable_redis_all_addr[self.ip].flushall(True)
            self.cache_redis.flushall(True)
            remain_keys_len = len(self.cache_redis.keys("*")) 
            print(f"clearing caches, remaining:{remain_keys_len}")

    def clear_db(self, transaction_id):
        db = self.couch['results']
        db.delete(db[transaction_id])

    def log_status(self, workflow_name, transaction_id, status):
        log_db = self.couch['log']
        log_db.save({'transaction_id': transaction_id, 'workflow': workflow_name, 'status': status})
    
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)

    def param_wrapper(self, transaction_id, mode, func="" ,key=""):
        return f"{transaction_id}:{mode}:{func}:{key}" 
    
    def param_decode(self, redis_key):
        parts = redis_key.split(':')
        key = ':'.join(parts[3:])  # 将索引3及之后的部分重新组合成key
        return key

    # input_keys: specify the keys you want
    def fetch_result(self, transaction_id, func, output):
        keys = output.keys()
        print(f"fetching result. Keys: {keys}")
        result = {}
        for k in keys:
            redis_key = self.param_wrapper(transaction_id, 'RET', func, k)
            if output[k]["type"] == "int":
                result[k] = int(self.shadowtable_redis_all_addr[self.ip][redis_key])
            else:
                result[k] = self.shadowtable_redis_all_addr[self.ip][redis_key]
        return result

    def get_global_function_pos(self, batch_id):
        func_pos_key =  self.param_wrapper(batch_id, 'POS')
        # get the function position from redis
        func_pos = json.loads(self.shadowtable_redis_all_addr[self.ip][func_pos_key])
        return func_pos

    # commit keys to DB, flush cache, and delete shadow table entries.
    def commit_tx_writes(self, commit_keys):
        for redis_key in commit_keys:
            value = self.cache_redis.get(redis_key)
            # 调用 store_key_to_db 存储到数据库中
            self.data_db.store_data_to_db(redis_key, value)
