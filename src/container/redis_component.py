from gevent import monkey
monkey.patch_all()
import redis
import container_config
import json

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
        response = self.data_db.get_item(
            Key={
                'key': key
            }
        )
        item = response.get('Item')
        return  {"value": item['value'], "version": item['version']}