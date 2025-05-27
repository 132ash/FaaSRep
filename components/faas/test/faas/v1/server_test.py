import requests
import threading
import time

def send_request(idx):
    start = time.time()
    r = requests.get('http://127.0.0.1:9000/test')
    print(f"Thread {idx} got response in {time.time() - start:.4f}s: {r.json()}")

if __name__ == '__main__':
    thread_num = 5
    threads = []
    start = time.time()
    for i in range(thread_num):
        t = threading.Thread(target=send_request, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print(f"Total time for {thread_num} requests: {time.time() - start:.4f}s")