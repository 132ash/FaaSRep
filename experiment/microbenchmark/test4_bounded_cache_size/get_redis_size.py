import redis

# 设置 Redis 客户端
redis_client_shadow = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
redis_client_cache = redis.StrictRedis(host="127.0.0.1", port=6380, db=1)


def check_redis_data():
    # 获取所有键
    # 获取数据库大小
    db_size = redis_client_cache.dbsize()
    print(f"Redis cache database size: {db_size}")

    # 获取所有键
    keys = redis_client_cache.keys("*")
    print(f"Total keys: {len(keys)}")

    # 打印前10个键和它们的值
    for i, key in enumerate(keys[:10]):
        try:
            value = redis_client_cache.get(key)
            print(f"Key {i+1}: {key} -> {value}")
        except Exception as e:
            print(f"Key {i+1}: {key} -> Error reading value: {e}")


if __name__ == "__main__":
    check_redis_data()