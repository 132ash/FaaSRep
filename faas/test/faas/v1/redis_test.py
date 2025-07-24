import redis

# 设置 Redis 客户端
redis_client_shadow = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
redis_client_cache = redis.StrictRedis(host="127.0.0.1", port=6379, db=1)

print(redis_client_shadow.get("sds"))

def check_redis_data():
    # 获取所有键
    keys = redis_client_cache.keys("*")
    
    if not keys:
        print("No keys found in Redis database.")
        return
    # 打印每个键及其对应的值
    for key in keys:
        # value = redis_client_cache.get(key)
        print(f"Key: {key}")
        # redis_client.delete(key)

def check_transaction_data():
    # 获取所有键
    keys = redis_client_shadow.keys("*")
    if not keys:
        print("No keys found in Redis database.")
        return
    # 打印每个键及其对应的值
    for key in keys:
        print(f"Key: {key}")

    pipe = redis_client_shadow.pipeline()
    pipe.multi()
    pipe.get("test_key")
    pipe.rpush("test_list", "value1")
    responses = pipe.execute()
    for i in range(len(responses)):
        print(f"Response {i}: {responses[i]}")
    
    

if __name__ == "__main__":
    check_transaction_data()
