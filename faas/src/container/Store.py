from gevent import monkey
monkey.patch_all()
import os
import threading
from beldi_store import BeldiStore
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
        self.io_latency = 0
        self.lock_latency = 0

        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    def init(self, function_name,  db_server):
        self.function_name = function_name
        self.db_server = db_server
        self.beldi_store = BeldiStore(self.db_server)

    def runtime_init(self, input, output, transaction_id, metadata):
        self.transaction_id = transaction_id
        self.input = input
        self.output = output
        self.write_set = metadata['write_set']
        self.lock_set = metadata['lock_set']
        self.beldi_store.runtime_init(transaction_id, self.lock_set)

    # mode: 'RET', 'PUT'
    def param_wrapper(self, func , key, mode):
        return f"{mode}:{func}:{key}"
        
    def abort_tx(self):
        raise Exception("Transaction abort triggered by itself.")

    def fetch_from_mem(self, k, param_key):
        value, _ = self.beldi_store.get(param_key, self.function_name)
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
    def put_to_mem(self, k, target_func, mode):
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
        upstream_func = self.write_set.get(key, "")
        value, lock_time = self.beldi_store.get(key, upstream_func)
        self.lock_latency += lock_time
        end = time.time()  
        self.io_latency += (end - start)
        return value
    
    def put(self, key, value):
        start = time.time()
        upstream_func = self.write_set.get(key, "")
        lock_time = self.beldi_store.put(key, value, self.function_name, upstream_func, self.write_set)
        self.lock_latency += lock_time
        end = time.time()  
        self.io_latency += (end - start)
