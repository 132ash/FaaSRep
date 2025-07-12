from gevent import monkey
monkey.patch_all()
import redis
import sys
import json
from typing import Dict
import threading

sys.path.append('../../config')
import config

RUNNING = config.RUNNING
ABORTED = config.ABORTED
REPAIRED = config.REPAIRED
    
class SubjectionCollector:
    def __init__(self, shadow_table:Dict[str, redis.StrictRedis], ip, cache_redis, db_server):
        self.shadow_table = shadow_table
        self.ip = ip
        self.cache_redis = cache_redis
        self.data_db = db_server

    def update_and_fetch(self, key):
        version, value = self.data_db.get_data_from_db(key)
        data = {"value": value, "version": version}
        self.cache_redis[key] = json.dumps(data)
        return data

    def set_state_and_get_waiting_downstream(self, tx_id, self_function, state):
        self_pipeline = self.shadow_table[self.ip].pipeline()
        self_pipeline.multi()
        self_pipeline.set(f"{tx_id}:STATE:{self_function}", state)
        self_pipeline.lpop(f"{tx_id}:SUCCESSOR:{self_function}:INFO", 0, -1)
        responses = self_pipeline.execute()
        downstream_funcs = responses[1]  # This is the list of downstream functions waiting for this function's state
        for i, info_str in enumerate(downstream_funcs):
            downstream_funcs[i] = info_str.split(':')  # Split the string into a list [tx_id, func, ip, port]
        return downstream_funcs
    
    def send_data_to_waiting_downstream(self, self_tx_id, self_function, downstream_funcs):
        self_redis = self.shadow_table[self.ip]
        downstream_keys = {}  # {ip: [(tx_id, func, key), ...]}
        # f"{transaction_id}:{self_function}:UPSTREAM:"
        for info in downstream_funcs:
            tx_id, func, ip = info[0], info[1], info[2]
            keys = self_redis.lpop(f"{self_tx_id}:SUCCESSOR:{self_function}:KEYS:{tx_id}:{func}", 0, -1)
            for key in keys:
                downstream_keys.setdefault(ip, []).append((tx_id, func, key))
        # 2. 多线程并发写入每个 ip 的 redis
        def send_keys_to_ip(ip):
            pipeline = self.shadow_table[ip].pipeline()
            pipeline.multi()
            for tx_id, func, key in downstream_keys[ip]:
                # 获取本地 redis 中的数据
                value = self.shadow_table[self.ip].get(f"{self_tx_id}:PUT:{self_function}:{key}")
                # 写入目标 redis
                pipeline.set(f"{tx_id}:{func}:UPSTREAM:{key}", value)
            pipeline.execute()

        threads = []
        for ip in downstream_keys:
            t = threading.Thread(target=send_keys_to_ip, args=(ip,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    # fetch all upstream keys from redis, and append self function into the successor list.
    def fetch_upstream_keys(self, upstream_keys_info, self_tx_id, self_function):
        upstream_redis_pipelines = {} # {ip: pipelines}
        upstream_fetch_results = {} # {ip:{txid: {func: {state:xx, fetched_keys:{key: res}}}}}

        for key, upstream_info in upstream_keys_info.items():
            upstream_txid = upstream_info['txid']
            upstream_func = upstream_info['func']
            upstream_ip = upstream_info['ip']
            upstream_fetch_results.setdefault(upstream_ip, {}).setdefault(upstream_txid, {}).setdefault(upstream_func, {'state':'', 'fetched_keys':{}})['fetched_keys'][key] = ''

        for upstream_ip, upstream_tx_dict in upstream_fetch_results.items():
            pipeline = self.shadow_table[upstream_ip].pipeline()
            pipeline.multi()
            for upstream_txid, upstream_func_dict in upstream_tx_dict.items():
                for upstream_func, func_result_info in upstream_func_dict.items():
                    pipeline.get(f"{upstream_txid}:STATE:{upstream_func}")
                    pipeline.rpush(f"{upstream_txid}:SUCCESSOR:{upstream_func}:INFO", f"{self_tx_id}:{self_function}:{self.ip}")
                    for key in func_result_info['fetched_keys']:
                        pipeline.rpush(f"{upstream_txid}:SUCCESSOR:{upstream_func}:KEYS:{self_tx_id}:{self_function}", key)
                        redis_data_key = f"{upstream_txid}:PUT:{upstream_func}:{key}"
                        pipeline.get(redis_data_key)
            upstream_redis_pipelines[upstream_ip] = pipeline

        def fetch_and_fill(ip):
            responses = upstream_redis_pipelines[ip].execute()
            idx = 0
            for _, upstream_func_dict in upstream_fetch_results[ip].items():
                for _, func_result_info in upstream_func_dict.items():
                    # 第一个是state
                    func_result_info['state'] = responses[idx]
                    idx += 1
                    # 第二个是rpush的返回值（可以忽略或记录）
                    idx += 1
                    # 后面是各key
                    for key in func_result_info['fetched_keys']:
                        func_result_info['fetched_keys'][key] = responses[idx]
                        idx += 1
        threads = []
        for ip in upstream_redis_pipelines:
            t = threading.Thread(target=fetch_and_fill, args=(ip,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        return upstream_fetch_results

    def prepair_subjection_before_repair(self, transaction_id, function_name, keys_from_upstream, upstream_fetch_results):
        upstream_waiting = 0
        set_pipeline = self.shadow_table[self.ip].pipeline()
        set_pipeline.multi()
        for _, upstream_tx_dict in upstream_fetch_results.items():
            for _, upstream_func_dict in upstream_tx_dict.items():
                for _, func_result_info in upstream_func_dict.items():
                    if func_result_info['state'] is None:
                        # If the state is None, it means the function has been commited. trigger cache update.
                        for key in func_result_info['fetched_keys'].keys():
                            keys_from_upstream.pop(key)
                            self.update_and_fetch(key)
                    elif func_result_info['state'] == RUNNING:
                        # If upstream function is still running, this func should wait for it.
                        upstream_waiting += 1
                    elif func_result_info['state'] == REPAIRED:
                        # update the fetched keys in shadow table. add fetch count.
                        upstream_key_prefix = f"{transaction_id}:{function_name}:UPSTREAM:"
                        for key, value in func_result_info['fetched_keys'].items():
                            set_pipeline.set(upstream_key_prefix+key, value)
        set_pipeline.execute()
        return upstream_waiting
   