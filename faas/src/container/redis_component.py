from gevent import monkey
monkey.patch_all()
import redis
import container_config
import json
import threading


class RedisShadowTable:
    def __init__(self, host_list, port, db, ip):
        self.redis = {
                    host:redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
                        for host in host_list
                    }
        self.ip = ip
        
    def put(self, key, ip, value):
        self.redis[ip][key] = value

    def self_put(self, key, value):
        self.redis[self.ip].set(key, value)

    def self_get(self, key):
        value = self.redis[self.ip].get(key)
        return value

    def raw_fetch_data(self, redis_key, ip):
        return self.redis[ip][redis_key]
            
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
    
class RepairSidecar:
    def __init__(self, function, shadow_table: RedisShadowTable, cache: RedisCache, ip):
        self.shadow_table = shadow_table
        self.cache = cache
        self.ip = ip
        self.function = function

    def state_change_and_nofify_downstream(self, tx_id, func, state):
        downstream_redis_pipelines = {} # {ip: pipelines}
        self_pipeline = self.shadow_table.redis[self.ip].pipeline()
        # TODO: change state and notify downstream
  
    # fetch all upstream keys from redis, and append self function into the successor list.
    def fetch_upstream_keys(self, upstream_keys_info, self_tx_id):
        upstream_redis_pipelines = {} # {ip: pipelines}
        upstream_fetch_results = {} # {ip:{txid: {func: {state:xx, fetched_keys:{key: res}}}}}

        for key, upstream_info in upstream_keys_info.items():
            upstream_txid = upstream_info['txid']
            upstream_func = upstream_info['func']
            upstream_ip = upstream_info['ip']
            upstream_fetch_results.setdefault(upstream_ip, {}).setdefault(upstream_txid, {}).setdefault(upstream_func, {'state':'', 'fetched_keys':{}})['fetched_keys'][key] = ''

        for upstream_ip, upstream_tx_dict in upstream_fetch_results.items():
            pipeline = self.shadow_table.redis[upstream_ip].pipeline()
            pipeline.multi()
            for upstream_txid, upstream_func_dict in upstream_tx_dict.items():
                for upstream_func, func_result_info in upstream_func_dict.items():
                    pipeline.get(f"{upstream_txid}:STATE:{upstream_func}")
                    pipeline.rpush(f"{upstream_txid}:SUCCESSOR:{upstream_func}", f"{self_tx_id}:{self.function}")
                    for key in func_result_info['fetched_keys']:
                        redis_data_key = f"{upstream_txid}:PUT:{upstream_func}:{key}"
                        pipeline.get(redis_data_key)
            upstream_redis_pipelines[upstream_ip] = pipeline

        def fetch_and_fill(ip):
            responses = upstream_redis_pipelines[ip].execute()
            idx = 0
            for upstream_txid, upstream_func_dict in upstream_fetch_results[ip].items():
                for upstream_func, func_result_info in upstream_func_dict.items():
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
