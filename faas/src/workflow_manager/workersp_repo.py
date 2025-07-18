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
            return item['version'], item['value']
        else:
            return None, None

    def store_data_to_db(self, key, version, value):
        self.table.put_item(
            Item={
                'key': key,
                'version': version,
                'value': value
            }
        )

class Repository:
    def __init__(self):
        self.cache_redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.CACHE_DB)
        self.data_db = DynamoDBClient(dynamodb_url, dynamodb_access_key, dynamodb_key_id, dynamodb_area)
        self.couch = couchdb.Server(couchdb_url)
        self.shadowtable_redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB, decode_responses=True)
                    
        
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
            keys = self.shadowtable_redis.keys(f"{transaction_id}:*")
            if keys:
                pipe = self.shadowtable_redis.pipeline()
                for key in keys:
                    pipe.delete(key)
                pipe.execute()
        else:
            print("clearing all shadow tables and cache")
            self.shadowtable_redis.flushall(True)
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
                result[k] = int(self.shadowtable_redis[redis_key])
            else:
                result[k] = self.shadowtable_redis[redis_key]
        return result

    def update_cache(self, keys, version='', from_db=True):
        if from_db:
            for key in keys:
                version, value = self.data_db.get_data_from_db(key)
                data = {"value": value, "version": version}
                self.cache_redis[key] = json.dumps(data)
        else:
            pipe = self.cache_redis.pipeline()
            for key in keys:
                value = self.shadowtable_redis.get(key)
                if value:
                    data = {"value": value, "version": version}
                    pipe.set(key, json.dumps(data))
            pipe.execute()

    def fillup_repair_matadata(self, repair_metadata):
        for txid in repair_metadata:
            for func in repair_metadata[txid]:
                # fill up the repair metadata to redis
                repair_info = repair_metadata[txid][func]
                repair_info
                redis_key = self.param_wrapper(txid, 'REPAIR', func, "")
                self.shadowtable_redis[redis_key] = json.dumps(repair_metadata[txid][func])

    def get_global_function_pos(self, batch_id):
        func_pos_key =  self.param_wrapper(batch_id, 'POS')
        # get the function position from redis
        func_pos = json.loads(self.shadowtable_redis[func_pos_key])
        return func_pos

    # commit keys to DB, flush cache, and delete shadow table entries.
    def commit_tx_writes(self, commit_key_list, tx_list, version):
        if config.FaaSTCC:
            keys = self.shadowtable_redis.keys(f"{tx_list[0]}:PUT:*")
            shadow_table_pipe = self.shadowtable_redis.pipeline()
            shadow_table_pipe.multi()
            for redis_key in keys:
                shadow_table_pipe.get(redis_key)
            responses = shadow_table_pipe.execute()
            for redis_key, value in zip(keys, responses):
                key = self.param_decode(redis_key)
                self.data_db.store_data_to_db(key, version, value)
        else:
            cache_pipe = self.cache_redis.pipeline()
            cache_pipe.multi()
            for redis_key in commit_key_list:
                key = self.param_decode(redis_key)
                value = self.shadowtable_redis.get(redis_key)
                cache_pipe.set(redis_key, json.dumps({"value": value, "version": version}))
                # 调用 store_key_to_db 存储到数据库中
                self.data_db.store_data_to_db(key, version, value)
            cache_pipe.execute()

    def fillup_cache(self):
        data = self.data_db.get_all_data_from_db()
        expired_version = datetime(1970, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')
        for item in data:
            key = item['key']
            if config.EXPIRED_CACHE:
                # eariler than default timestamp in database
                version = expired_version
            else:
                version = item['version']
            self.cache_redis[key] = json.dumps({"value": item['value'], "version": version})
        print(f"cache filled up expired_cache:{config.EXPIRED_CACHE}. Waiting for request.")

    def release_lock(self, transaction_id, lock_set):
        """
        释放 lock_set 中每个 key 的锁，将其 lock 属性设置为 None。
        """

        for key in lock_set.keys():
            # 更新 lock 属性为 None
            self.data_db.update_item(
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
