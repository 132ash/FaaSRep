from gevent import monkey
monkey.patch_all()
from redis_component import RedisShadowTable, RedisCache
from shadow_client import BokiClient
from transaction_errors import ActiveAbortException
import os
import threading
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
    def __init__(self):
        self.fetch_dict = {}
        self.ret_dict = {}
        self.redis_shadow_table: RedisShadowTable = None
        self.redis_cache: RedisCache = None


        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    def init(self, function_name, shadow_table:RedisShadowTable, cache:RedisCache, db_server, function_pos,input, output):
        self.function_name = function_name
        self.redis_shadow_table = shadow_table
        self.redis_cache = cache
        self.function_pos = function_pos
        self.db_server = db_server
        self.input = input
        self.output = output

    def runtime_init(self, transaction_id, metadata):
        self.transaction_id = transaction_id
        self.term = int(metadata.get('term', 0))
        self.redis_shadow_table.runtime_init(transaction_id, self.term)
        self.read_set = metadata['read_set']
        self.write_set = metadata['write_set']
        self.cache_enable = metadata.get('cache_enable', False)
        self.io_latency = 0
        self.boki = None
        if metadata.get('system_mode') == 'BOKI_SN':
            self.cache_enable = False
            self.boki = BokiClient(transaction_id, self.term, int(metadata['birth_seq']), self.function_name)

    # mode: 'RET', 'PUT'
    def param_wrapper(self, func, key, mode, txid=None):
        if self.cache_enable:
            if txid:
                return f"{txid}:{self.term}:{mode}:{func}:{key}"
            else:
                return f"{self.transaction_id}:{self.term}:{mode}:{func}:{key}"
        else:
            return f"{mode}:{self.term}:{func}:{key}"
        
    def get_redis_ip(self, upstream):
        if upstream == "GLOBAL":
            return self.function_pos[self.function_name]
        else:
            return self.function_pos[upstream]
        
    def abort_tx(self, message):
        raise ActiveAbortException(str(message))

    def fetch_from_mem(self, k, param_key, upstream, param_type):
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
        # print(f"fetch input from mem: {self.fetch_dict}")
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
        if self.boki is not None:
            self.boki.lock(key, 'S')
            hit, value = self.boki.get(key)
            if hit:
                return value
            start = time.time()
            _, value = self.redis_cache.db_get(key)  # consistent main-table read; no shared cache
            elapsed = time.time() - start
            self.io_latency += elapsed
            self.boki.metrics['db_io_latency'] += elapsed
            return value
        value = None
        start = time.time()
        # first run, check RYW subjection.
        upstream_func = self.write_set.get(key, "")
        if upstream_func:
            upstream_ip = self.function_pos[upstream_func]
            value = self.redis_shadow_table.raw_fetch_data(self.param_wrapper(upstream_func, key, 'PUT'), upstream_ip)
        else:
            value_version_pair =  self.redis_cache.cache_get(key)
            self.read_set[key] = value_version_pair["version"]
            value = value_version_pair["value"]
        end = time.time()
        self.io_latency += (end - start)
        return value
    
    def put(self, key, value):
        if self.boki is not None:
            self.boki.lock(key, 'X')
            self.boki.put(key, value)
            return
        start = time.time()
        self.put_to_mem(key, self.function_name, 'PUT', value)
        self.write_set[key] = self.function_name
        end = time.time()
        self.io_latency += (end - start)

