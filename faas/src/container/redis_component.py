import redis
import container_config
import json

class RedisShadowTable:
    def __init__(self,function, host_list, port, db):
        self.redis = {
                    host:redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
                        for host in host_list
                    }
        self.function = function
        
    def put(self, key, ip, value):
        self.redis[ip][key] = value

    def raw_fetch_data(self, redis_key, ip):
        res = self.redis[ip][redis_key]
        return res
        
    def fetch_upstream(self, upstream_txid, upstream_func, upstream_ip, self_tx_id, redis_data_key):
        upstream_successors = f"{upstream_txid}:{upstream_func}:SUCCESSOR"
        self_pos_info = f"{self_tx_id}:{self.function}"
        upstream_func_state = f"{upstream_txid}:{upstream_func}:STATE"
        upstream_redis_pipe = self.redis[upstream_ip].pipeline()
        # start redis transaction: get upstream state, append self into successor list, and get data.
        upstream_redis_pipe.multi()
        upstream_redis_pipe.get(upstream_func_state)
        upstream_redis_pipe.rpush(upstream_successors, self_pos_info)
        upstream_redis_pipe.get(redis_data_key)
        responses = upstream_redis_pipe.execute()
        return responses
    
        
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