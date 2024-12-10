import json
import os
import threading
import time

import redis

class RedisClient:
    def __init__(self, host_list, port, db):
        self.redis = {
                    host : redis.StrictRedis(host=host, port=port, db=db)
                        for host in host_list
                    }

class Store:
    def __init__(self, workflow_name, function_name, transaction_id, input, output, function_pos, redis_server: RedisClient):
        self.redis_server = redis_server
        self.fetch_dict = {}
        self.ret_dict = {}
        self.workflow_name = workflow_name
        self.function_name = function_name
        self.transaction_id = transaction_id
        self.function_pos = function_pos
        self.input = input
        self.output = output

        if os.path.exists('work'):
            os.system('rm -rf work')
        os.mkdir('work')

    def param_wrapper(self, func , key):
        return f"{self.transaction_id}:{func}:{key}" 
    
    def get_redis_ip(self, upstream):
        if upstream == "GLOBAL":
            return self.function_pos[self.function_name]
        else:
            return self.function_pos[upstream]


    def fetch_from_mem(self, k, redis_key, ip, param_type):
        redis_value = self.redis_server.redis[ip][redis_key].decode('utf-8')
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
            redis_key = self.param_wrapper(upstream, k)
            ip = self.get_redis_ip(upstream)
            thread_ = threading.Thread(target=self.fetch_from_mem, args=(k, redis_key, ip, param_type))
            threads.append(thread_)
        for thread_ in threads:
            thread_.start()
        for thread_ in threads:
            thread_.join()
        return self.fetch_dict

    # return to local redis.
    def put_to_mem(self, k, ip):
        redis_key = self.param_wrapper(self.function_name, k)
        self.redis_server.redis[ip][redis_key] = self.ret_dict[k]

    # output_result: {'k': 'value'}
    # output_content_type: default application/json, just specify one when you need to
    def ret(self, output_result):
        ip = self.function_pos[self.function_name]
        for k, v in output_result.items():
            self.ret_dict[k] = v
            self.put_to_mem(k, ip)
            
      