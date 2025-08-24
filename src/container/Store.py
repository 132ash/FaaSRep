from gevent import monkey
monkey.patch_all()
from redis_component import RedisShadowTable, RedisCache
import requests
import os
import threading
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

class ActiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "ACTIVE"
        self.message = message

class PassiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "PASSIVE"
        self.message = message

class Store:
    def __init__(self):
        self.fetch_dict = {}
        self.ret_dict = {}
        self.io_latency = 0
        self.redis_shadow_table: RedisShadowTable = None
        self.redis_cache: RedisCache = None
        self.lock_latency = 0
        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    def init(self, host_addr, workflow_name, function_name, shadow_table:RedisShadowTable, cache:RedisCache, db_server, function_pos):
        self.concord_cache_addr = f'{host_addr}:6000/concord_data'
        self.workflow_name = workflow_name
        self.function_name = function_name
        self.redis_shadow_table = shadow_table
        self.redis_cache = cache
        self.function_pos = function_pos
        self.db_server = db_server

    def runtime_init(self, input, output, transaction_id, metadata):
        self.io_latency = 0
        self.transaction_id = transaction_id
        self.input = input
        self.output = output
        self.write_set = metadata['write_set']
        self.term = metadata['term']

    # mode: 'RET', 'PUT'
    def param_wrapper(self, func , key, mode, txid=None):
        if txid:
            return f"{txid}:{mode}:{func}:{key}" 
        else:
            return f"{self.transaction_id}:{mode}:{func}:{key}" 
    
    def get_redis_ip(self, upstream):
        if upstream == "GLOBAL":
            return self.function_pos[self.function_name]
        else:
            return self.function_pos[upstream]
        
    def abort_tx(self, message):
        raise ActiveAbortException(f"Transaction abort triggered by itself: {message}")

    def fetch_from_mem(self, k, param_key, upstream, param_type):
        ip = self.get_redis_ip(upstream)
        value = self.redis_shadow_table.raw_fetch_data(param_key, ip)
        if param_type == "int":
            value = int(value)
        self.fetch_dict[k] = value

    def fetch_input(self):
        res = self.fetch(self.input.keys())
        return res

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
        # # logging.info(f"fetch input from mem: {self.fetch_dict}")
        return self.fetch_dict

    # return to local redis.
    def put_to_mem(self, k, target_func, mode, value=None):
        ip = self.get_redis_ip(target_func)
        redis_key = self.param_wrapper(self.function_name, k, mode)
        if mode == 'RET':
            self.redis_shadow_table.put(redis_key, ip, self.ret_dict[k])
        else:
            self.redis_shadow_table.put(redis_key, ip, value)

    # output_result: {'k': 'value'}
    # output_content_type: default application/json, just specify one when you need to
    def ret(self, output_result):
        for k, v in output_result.items():
            self.ret_dict[k] = v
            self.put_to_mem(k, self.function_name, 'RET')

    def get(self, key):
        value = None
        start = time.time()
        value = self.concord_get(key)
        end = time.time()
        self.io_latency += (end - start)
        return value
    
    def put(self, key, value):
        start = time.time()
        self.put_to_mem(key, self.function_name, 'PUT', value)
        self.write_set[key] = self.function_name
        self.concord_put(key, value)
        end = time.time()
        self.io_latency += (end - start)

    def concord_get(self, key):
        url = f"http://{self.concord_cache_addr}"
        data = {'mode':'read', 'key': key, 'trigger_tx': self.transaction_id, 'workflow': self.workflow_name, 'term':self.term}
        print(f"func {self.function_name} in {self.transaction_id} concord get key:{key}")
        response = requests.post(url, json=data).json()
        if not response['success']:
            logging.error(f"Concord cache get failed for key {key} in transaction {self.transaction_id}.")
            raise PassiveAbortException("Concord cache get failed.")
        # logging.info(f"Concord cache succeeded for key {key} in transaction {self.transaction_id}.")
        return response['value']
        
    def concord_put(self, key, value):
        url = f"http://{self.concord_cache_addr}"
        data = {'mode':'write', 'key': key, 'trigger_tx': self.transaction_id, 'workflow': self.workflow_name, 'value': value, 'term':self.term}
        print(f"func {self.function_name} in {self.transaction_id} concord put key:{key}")
        response = requests.post(url, json=data).json()
        if not response['success']:
            logging.error(f"Concord cache put failed for key {key} in transaction {self.transaction_id}.")
            raise PassiveAbortException("Concord cache put failed.")

