from gevent import monkey
monkey.patch_all()
from redis_component import RedisShadowTable, RedisCache
import json
import os
import threading
import container_config
from beldi_store import BeldiStore
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
            value, _ = self.beldi_store.get(param_key, self.function_name)
        else:
            ip = self.get_redis_ip(upstream)
            value = self.redis_shadow_table.raw_fetch_data(param_key, ip)
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
        # logging.info(f"fetch input from mem: {self.fetch_dict}")
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
            value, lock_time = self.beldi_store.get(key, upstream_func)
            self.lock_latency += lock_time
            end = time.time()  
        else: 
            # first run, check RYW subjection.
            if not self.is_repair:
                upstream_func = self.write_set.get(key, "")
                if upstream_func:
                    upstream_ip = self.function_pos[upstream_func]['ip']
                    value = self.redis_shadow_table.fetch(self.param_wrapper(upstream_func, key, 'PUT'), upstream_ip)
                    self.RYW_subjection[key] = upstream_func
            # SECOND run or not RYW, read from cache.
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
            lock_time = self.beldi_store.put(key, value, self.function_name, upstream_func, self.write_set)
            self.lock_latency += lock_time
            end = time.time()  
        else:       
            self.put_to_mem(key, self.function_name, 'PUT', value)
            self.write_set[key] = self.function_name
            end = time.time()
        self.io_latency += (end - start)