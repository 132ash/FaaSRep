from gevent import monkey
monkey.patch_all()
import redis
import container_config
import json
import threading
RUNNING = container_config.RUNNING
ABORTED = container_config.ABORTED
REPAIRED = container_config.REPAIRED

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
    def __init__(self, port, db, db_server, cache_enable):
        self.redis = redis.StrictRedis(host=container_config.CACHE_HOST, port=port, db=db)
        self.cache_enable = cache_enable
        self.data_db = db_server.Table('data')

    def cache_get(self, key):
        if not self.cache_enable:
            version, value = self.db_get(key)
            return  {"value": value, "version": version}
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
