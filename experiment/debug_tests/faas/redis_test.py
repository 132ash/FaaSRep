import redis

# 设置 Redis 客户端
redis_client_shadow = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
redis_client_cache = redis.StrictRedis(host="127.0.0.1", port=6380, db=1)


def check_redis_data():
    # 获取所有键
    keys = redis_client_shadow.keys("*")
    ret_file = "redis_data_check.txt"
    f = open(ret_file, "a")
    if not keys:
        print("No keys found in Redis database.")
        return
    # 打印每个键及其对应的值
    # print(redis_client_shadow.get("5d92e54e-4ae7-4ba6-a4df-5ebaaa8950d8:UPSTREAM:f1:t2"))
    for key in keys:
        # value = redis_client_cache.get(key)
        f.write(f"Key: {key}, Value: {redis_client_shadow.get(key)}\n")
        print(f"Key: {key}, value: {redis_client_shadow.get(key)}")
        # redis_client.delete(key)

def check_transaction_data():
    # 获取所有键
    redis_client_shadow.set("test_key", "test_value")

    pipe = redis_client_cache.pipeline()
    pipe.multi()
    pipe.get("test_key")
    pipe.rpush("test_list", "value1")
    responses = pipe.execute()
    for i in range(len(responses)):
        print(f"Response {i}: {responses[i]}")
    
    

if __name__ == "__main__":
    check_redis_data()
