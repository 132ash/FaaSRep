import requests
import threading
import time
import random

SERVER_URL = "http://127.0.0.1:9000/test"
NUM_REQUESTS = 13

results = [None] * NUM_REQUESTS

def send_request(pool_id, op, key, value, idx):
    payload = {
        "pool_id": pool_id,
        "op": op,
        "key": key,
        "value": value
    }
    start = time.time()
    try:
        resp = requests.post(SERVER_URL, json=payload, timeout=10)
        elapsed = time.time() - start
        print(f"[{idx}] Response: {resp.json()} | Time: {elapsed:.3f}s")
        results[idx] = elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"[{idx}] Error: {e} | Time: {elapsed:.3f}s")
        results[idx] = None

def main():
    threads = []
    t0 = time.time()
    for i in range(NUM_REQUESTS):
        pool_id = random.randint(0, 1)  # Randomly choose a pool ID (0 or 1)
        op = "set" if i % 2 == 0 else "get"
        key = f"k{i%3}"
        value = i
        t = threading.Thread(target=send_request, args=(pool_id, op, key, value, i))
        threads.append(t)
        t.start()
        # 不sleep，全部并发发起
    for t in threads:
        t.join()
    t1 = time.time()
    valid_times = [r for r in results if r is not None]
    if valid_times:
        print(f"\nTotal time for {NUM_REQUESTS} requests: {t1-t0:.3f}s")
        print(f"Max: {max(valid_times):.3f}s, Min: {min(valid_times):.3f}s, Avg: {sum(valid_times)/len(valid_times):.3f}s")
    else:
        print("No valid responses.")

if __name__ == "__main__":
    main()