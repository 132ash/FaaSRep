import redis

# 设置 Redis 客户端
redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0)
redis_client1 = redis.StrictRedis(host="127.0.0.1", port=6379, db=1)

print(redis_client1.get("sds"))

def check_redis_data():
    # 获取所有键
    keys = redis_client.keys('*')
    
    if not keys:
        print("No keys found in Redis database.")
        return
    
    # 打印每个键及其对应的值
    for key in keys:
        value = redis_client.get(key)
        print(f"Key: {key.decode('utf-8')}, Value: {value.decode('utf-8')}")
        # redis_client.delete(key)

if __name__ == "__main__":
    check_redis_data()
    redis_client["sds"] = 1
    check_redis_data()