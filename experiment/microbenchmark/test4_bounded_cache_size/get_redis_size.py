import redis

# 设置 Redis 客户端
redis_client_shadow = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
redis_client_cache = redis.StrictRedis(host="127.0.0.1", port=6380, db=1)


def check_redis_data():
    try:
        # 获取 redis_client_cache 的内存占用信息
        memory_info = redis_client_cache.info('memory')
        used_memory = memory_info.get('used_memory')
        used_memory_human = memory_info.get('used_memory_human')

        print("--- Redis Cache Memory Usage ---")
        if used_memory is not None and used_memory_human is not None:
            print(f"Memory Usage: {used_memory_human} ({used_memory:,} bytes)")
        else:
            print("Could not retrieve memory usage information.")
        print("--------------------------------\n")

    except redis.exceptions.ResponseError as e:
        print(f"Error getting memory info: {e}. Is the 'INFO' command disabled?")
    except Exception as e:
        print(f"An error occurred while fetching memory info: {e}")

    # 获取数据库大小
    db_size = redis_client_cache.dbsize()
    print(f"Redis cache database size: {db_size}")

    # 获取所有键
    keys = redis_client_cache.keys("*")
    print(f"Total keys: {len(keys)}")

    # 打印前10个键和它们的值
    # print("\n--- Sample Keys ---")
    # for i, key_bytes in enumerate(keys[:10]):
    #     try:
    #         key = key_bytes.decode('utf-8', errors='ignore')
    #         value = redis_client_cache.get(key_bytes)
    #         print(f"Key {i+1}: {key} -> {value}")
    #     except Exception as e:
    #         print(f"Key {i+1}: {key_bytes} -> Error reading value: {e}")
    # print("-------------------")


if __name__ == "__main__":
    check_redis_data()