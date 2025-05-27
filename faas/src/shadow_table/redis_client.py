import redis

class RedisClient:
    def __init__(self, host_list, port, db):
        self.redis = {
                    host : redis.StrictRedis(host=host, port=port, db=db)
                        for host in host_list
                    }

    def get(self, key):
        return self.redis.get(key)

    def delete(self, key):
        self.redis.delete(key)

    def keys(self):
        return self.redis.keys()
