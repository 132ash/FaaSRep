import json
import os
import threading
import container_config
import redis
import time

class RedisShadowTable:
    def __init__(self, host_list, port, db):
        self.redis = {
                    host : redis.StrictRedis(host=host, port=port, db=db)
                        for host in host_list
                    }
        
    def put(self, key, ip, value):
        self.redis[ip][key] = value

    def fetch(self, redis_key, ip):
        res = ""
        try:
            res = self.redis[ip][redis_key].decode('utf-8')
        except KeyError:
            pass
        return res
    
   
        

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
    def __init__(self, workflow_name, function_name, transaction_id, input, output, function_pos, redis_shadow_table: RedisShadowTable, cache: RedisCache, TxMetaData:dict, is_repair=False):
        self.redis_shadow_table = redis_shadow_table
        self.redis_cache = cache
        self.fetch_dict = {}
        self.ret_dict = {}
        self.workflow_name = workflow_name
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
        # metadata used in repair mode
        self.is_repair = is_repair
        self.downstream_func_table = TxMetaData['downstream_func_table']
        self.function_pos_whole_batch = TxMetaData['function_pos_whole_batch']
        self.dirty = TxMetaData['dirty']


        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    # mode: 'RET', 'PUT'
    def param_wrapper(self, func , key, mode, txid=None):
        if txid:
            return f"{txid}:{mode}:{func}:{key}" 
        else:
            return f"{self.transaction_id}:{mode}:{func}:{key}" 
    
    def get_redis_ip(self, upstream):
        if upstream == "GLOBAL":
            return self.function_pos[self.function_name]['ip']
        else:
            return self.function_pos[upstream]['ip']


    def fetch_from_mem(self, k, redis_key, ip, param_type):
        redis_value = self.redis_shadow_table.fetch(redis_key, ip)
        if param_type == "int":
            redis_value = int(redis_value)
        self.fetch_dict[k] = redis_value

    def fetch_input(self):
        return self.fetch(self.input.keys())

    # input_keys: specify the keys you want
    def fetch(self, input_keys):
        self.fetch_dict = {}
        threads = []
        for k in input_keys:
            upstream = self.input[k]["from"]
            param_type =  self.input[k]["type"]
            redis_key = self.param_wrapper(upstream, k, 'RET')
            ip = self.get_redis_ip(upstream)
            thread_ = threading.Thread(target=self.fetch_from_mem, args=(k, redis_key, ip, param_type))
            threads.append(thread_)
        for thread_ in threads:
            thread_.start()
        for thread_ in threads:
            thread_.join()
        return self.fetch_dict

    # return to local redis.
    def put_to_mem(self, k, ip, mode, value=None):
        redis_key = self.param_wrapper(self.function_name, k, mode)
        if mode == 'RET':
            self.redis_shadow_table.put(redis_key, ip, self.ret_dict[k])
        else:
            self.redis_shadow_table.put(redis_key, ip, value)

    # output_result: {'k': 'value'}
    # output_content_type: default application/json, just specify one when you need to
    def ret(self, output_result):
        ip = self.function_pos[self.function_name]['ip']
        for k, v in output_result.items():
            self.ret_dict[k] = v
            self.put_to_mem(k, ip, 'RET')

    def get(self, key):
        value = None
        start = time.time()
        RYW_sign=False
        upstream_func = self.write_set.get(key, "")
        upstream_txid = None
        upstream_ip = ''
        if upstream_func:
            RYW_sign = True
            upstream_ip = self.function_pos[upstream_func]['ip']
        else:
            upstream_func_info = self.downstream_func_table.get(key, {})
            upstream_func = upstream_func_info.get("func")
            if upstream_func:
                upstream_txid = upstream_func_info.get("transaction_id")
                upstream_ip = self.function_pos_whole_batch[upstream_txid][upstream_func]['ip']

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
        if key not in self.write_set:
            self.write_set[key] = self.function_name
        self.put_to_mem(key, self.function_pos[self.function_name]['ip'], 'PUT', value)
        end = time.time()
        self.io_latency += (end - start)

            
      