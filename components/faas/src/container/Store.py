import json
import os
import threading
import container_config
from botocore.exceptions import ClientError
import redis
import time
import sys
import logging
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别为 INFO
    format='%(asctime)s [%(levelname)s] %(message)s',  # 日志格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ]
)

class RedisShadowTable:
    def __init__(self, host_list, port, db):
        self.redis = {
                    host : redis.StrictRedis(host=host, port=port, db=db)
                        for host in host_list
                    }
        
    def put(self, key, ip, value):
        self.redis[ip][key] = value

    def fetch(self, redis_key, ip):
        res = self.redis[ip][redis_key].decode('utf-8')
        return res
    
class BeldiStore:
    def __init__(self, transaction_id, db_server, lock_set):
        self.transaction_id = transaction_id
        self.db_server = db_server
        self.data_db = db_server.Table('data')
        self.lock_set = lock_set
        self.shadow_table = db_server.Table(f"{self.transaction_id}_shadow_table")

    def put(self, key, value, this_func="", upstream_func="", write_set={}, ret=False):
        success = False
        lock_time = 0
        # have upstream func: no need for lock. change the write func.
        if upstream_func:
            success = True
        else:
            success, lock_time = self.acquire_lock(key)
        if success:
            self.shadow_table.put_item(
                Item={
                    'key': key,
                    'value': str(value)
                }
            )
            if not ret:
                write_set[key] = this_func
            return True, lock_time
        else:
            return False, ""

        

    def get(self, key, upstream_func):
        item = None
        value = None
        lock_success = False
        lock_time = 0
        # RYW. not acquire lock, read from shadow table.
        if upstream_func:
            logging.info(f"RYW: {key}, upstream_func: {upstream_func}")
            response = self.shadow_table.get_item(
                    Key={
                        'key': key
                    }
                )
            item = response.get('Item')
            lock_success = True
        else:
            lock_success, lock_time = self.acquire_lock(key)
            if lock_success:
                response = self.data_db.get_item(
                    Key={
                        'key': key
                    }
                )
                item = response.get('Item')
        value = item['value'] if item else None
        if lock_success and item:
            return lock_success, value, lock_time
        else:
            return False, "", 0

    def acquire_lock(self, key):
        try:
            start = time.time()
            if self.lock_set.get(key, False):
                return True, time.time() - start
            else:
                self.lock_set[key] = True
                self.data_db.update_item(
                    Key={'key': key},
                    UpdateExpression="SET #l = :txid",
                    ConditionExpression="attribute_not_exists(#l) OR #l = :none OR #l = :txid",
                    ExpressionAttributeNames={
                        '#l': 'lock'
                    },
                    ExpressionAttributeValues={
                        ':txid': self.transaction_id,
                        ':none': None
                    },
                    ReturnValues="UPDATED_NEW"
                )
                response = self.data_db.get_item(
                    Key={
                        'key': key
                    }
                )
                item = response.get('Item')
                end = time.time()
                logging.info(f"acquire lock for {key},value:{item['value']}, lock:{item['lock']}")
                return True, end - start      
        except ClientError as e:
            logging.info(f"acquire lock for {key} failed, error: {e.response['Error']['Message']}")
            return False, 0 
        
# data in cache: value and version 
class RedisCache:
    def __init__(self, port, db, db_server):
        self.redis = redis.StrictRedis(host=container_config.CACHE_HOST, port=port, db=db)
        self.data_db = db_server.Table('data')

    def cache_get(self, key):
        value_tuple = self.redis.get(key)
        if value_tuple == None:
            value_tuple = self.update_and_fetch(key)
        else:
            value_tuple = json.loads(value_tuple.decode('utf-8'))
        return value_tuple
    
    def db_get(self, key):
        # 从dynamodb中获取数据
        response = self.data_db.get_item(
            Key={
                'key': key
            }
        )
        item = response.get('Item')
        if item:
            return item['version'], item['value']
        else:
            return None, None
    
    def update_and_fetch(self, key):
        version, value = self.db_get(key)
        data = {"value": value, "version": version}
        self.redis[key] = json.dumps(data)
        return data


class Store:
    def __init__(self, function_name, transaction_id, input, output, function_pos, redis_shadow_table: RedisShadowTable, cache: RedisCache, TxMetaData:dict, is_repair=False, fast_path_enabled=False, remote_lock_enabled=False, db_server=None):
        self.fast_path_enabled = fast_path_enabled
        self.redis_shadow_table = redis_shadow_table
        self.redis_cache = cache
        self.fetch_dict = {}
        self.ret_dict = {}
        self.function_name = function_name
        self.transaction_id = transaction_id
        self.function_pos = function_pos
        self.input = input
        self.output = output
        self.io_latency = 0
        # collect message in first run
        self.read_set = TxMetaData['read_set']
        self.write_set = TxMetaData['write_set']
        self.RYW_subjection = TxMetaData['RYW_subjection']
        self.RYW_upstream = TxMetaData['RYW_upstream']
        # metadata used in repair mode
        self.is_repair = is_repair
        self.RYW_table_fastpath = TxMetaData['RYW_table_fastpath']
        self.downstream_func_table = TxMetaData['downstream_func_table']
        self.function_pos_whole_batch = TxMetaData['function_pos_whole_batch']
        self.dirty = TxMetaData['dirty']
        # BeldiStore: used for locking mode.
        self.lock_set = TxMetaData['lock_set'] 
        self.beldi_store = BeldiStore(self.transaction_id, db_server,  self.lock_set )
        self.remote_lock_enabled = remote_lock_enabled
        self.lock_latency = 0


        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    # mode: 'RET', 'PUT'
    def param_wrapper(self, func , key, mode, txid=None):
        if self.remote_lock_enabled:
            return f"{mode}:{func}:{key}"
        else:
            if txid:
                return f"{txid}:{mode}:{func}:{key}" 
            else:
                return f"{self.transaction_id}:{mode}:{func}:{key}" 
    
    def get_redis_ip(self, upstream):
        if upstream == "GLOBAL":
            return self.function_pos[self.function_name]['ip']
        else:
            return self.function_pos[upstream]['ip']


    def fetch_from_mem(self, k, param_key, upstream, param_type):
        if self.remote_lock_enabled:
            _, value, _ = self.beldi_store.get(param_key, self.function_name)
        else:
            ip = self.get_redis_ip(upstream)
            value = self.redis_shadow_table.fetch(param_key, ip)
        if param_type == "int":
            value = int(value)
        self.fetch_dict[k] = value

    def fetch_input(self):
        return self.fetch(self.input.keys())

    # input_keys: specify the keys you want
    def fetch(self, input_keys):
        self.fetch_dict = {}
        threads = []
        for k in input_keys:
            upstream = self.input[k]["from"]
            param_type =  self.input[k]["type"]
            param_key = self.param_wrapper(upstream, k, 'RET')
            thread_ = threading.Thread(target=self.fetch_from_mem, args=(k, param_key, upstream, param_type))
            threads.append(thread_)
        for thread_ in threads:
            thread_.start()
        for thread_ in threads:
            thread_.join()
        return self.fetch_dict

    # return to local redis.
    def put_to_mem(self, k, target_func, mode, value=None):
        if not self.remote_lock_enabled:
            ip = self.get_redis_ip(target_func)
            redis_key = self.param_wrapper(self.function_name, k, mode)
            if mode == 'RET':
                self.redis_shadow_table.put(redis_key, ip, self.ret_dict[k])
            else:
                self.redis_shadow_table.put(redis_key, ip, value)
        else:
            dynamo_key = self.param_wrapper(target_func, k, mode)
            self.beldi_store.put(dynamo_key, self.ret_dict[k], "", self.function_name,{}, True)


    # output_result: {'k': 'value'}
    # output_content_type: default application/json, just specify one when you need to
    def ret(self, output_result):
        for k, v in output_result.items():
            self.ret_dict[k] = v
            self.put_to_mem(k, self.function_name, 'RET')

    def get(self, key):
        value = None
        start = time.time()
        if self.remote_lock_enabled:
            # RYW. read from shadow table.
            upstream_func = self.write_set.get(key, "")
            success, value, lock_time = self.beldi_store.get(key, upstream_func)
            if not success:
                raise KeyError(f"Failed to acquire lock for key {key}")
            else:
                self.lock_latency += lock_time
                end = time.time()  
        else: 
            RYW_sign=False
            upstream_func=''
            upstream_txid = None
            upstream_ip = ''
            if not self.is_repair:
                upstream_func = self.write_set.get(key, "")
            else:
                upstream_func = self.RYW_upstream.get(key, "") if not self.fast_path_enabled else self.RYW_table_fastpath.get("upstream", {}).get(key, "")

            if upstream_func:
                RYW_sign = True
                upstream_ip = self.function_pos[upstream_func]['ip']
            else:
                upstream_func_info = self.downstream_func_table.get('upstream_keys', {}).get(key, {})
                if upstream_func_info:
                    upstream_func = upstream_func_info["func"]
                    upstream_txid = upstream_func_info["transaction_id"]
                    upstream_ip = self.function_pos_whole_batch[upstream_txid][upstream_func]['ip'] if self.fast_path_enabled else upstream_func_info["ip"]

            if upstream_func:
                value = self.redis_shadow_table.fetch(self.param_wrapper(upstream_func, key, 'PUT', upstream_txid), upstream_ip)
                if not self.is_repair and RYW_sign and upstream_func != self.function_name:
                    self.RYW_subjection[upstream_func] = True
            else:
                value_version_pair =  self.redis_cache.cache_get(key)
                self.read_set[key] = value_version_pair["version"]
                value = value_version_pair["value"]
            end = time.time()
        self.io_latency += (end - start)
        return value
    
    def put(self, key, value):
        start = time.time()
        if self.remote_lock_enabled:
            upstream_func = self.write_set.get(key, "")
            success, lock_time = self.beldi_store.put(key, value, self.function_name, upstream_func, self.write_set)
            if not success:
                raise KeyError(f"Failed to acquire lock for key {key}")
            else:
                self.lock_latency += lock_time
                end = time.time()  
        else:       
            if key not in self.write_set:
                self.write_set[key] = self.function_name
            self.put_to_mem(key, self.function_pos[self.function_name]['ip'], 'PUT', value)
            end = time.time()
        self.io_latency += (end - start)

            
      