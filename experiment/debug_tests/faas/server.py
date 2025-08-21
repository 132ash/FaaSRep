from gevent import monkey, sleep, spawn
import gevent.queue
monkey.patch_all()

from flask import Flask, jsonify, request
import sys, os, uuid
from multiprocessing import Process, Queue, Pipe
import numpy as np
import time



app = Flask(__name__)

# --- 序列化器进程 ---
class SerializerProcess(Process):
    def __init__(self, req_queue, result_pipes):
        super().__init__()
        self.req_queue = req_queue
        self.result_pipes = result_pipes  # list, 每个处理者一个Pipe
        self.table = {}

    def run(self):
        log_file = open(f"SerializerProcess.log", "a")
        while True:
            try:
                msg = self.req_queue.get(timeout=1)
                log_file.write(f"Received request: {msg}\n")
                log_file.flush()
            except:
                gevent.sleep(0.005)
                continue
            if msg == "STOP":
                break
            handler_id, req_id, op, key, value = msg
            if op == "set":
                self.table[key] = value
                result = (req_id, "ok", dict(self.table))
            elif op == "get":
                result = (req_id, self.table.get(key, None), dict(self.table))
            else:
                result = (req_id, "unknown op", dict(self.table))
            # 通过Pipe返回给对应的处理者
            self.result_pipes[handler_id].send(result)
            log_file.write(f"Serialized request {req_id} with op {op}, key {key}, value {value}\n")
            log_file.flush()

# --- 处理者进程 ---
class HandlerProcess(Process):
    def __init__(self, pool_id, handler_id, task_queue, serializer_req, result_pipe):
        super().__init__()
        self.pool_id = pool_id
        self.handler_id = handler_id
        self.task_queue = task_queue      # Queue: 主进程->处理者
        self.serializer_req = serializer_req
        self.result_pipe = result_pipe

    def run(self):
        log_file = open(f"{self.pool_id}_handler_{self.handler_id}.log", "a")
        while True:
            msg = self.task_queue.get()
            log_file.write(f"Handler {self.handler_id} received task: {msg}\n")
            log_file.flush()
            if msg == "STOP":
                break
            req_id, op, key, value = msg
            start = time.time()
            self.serializer_req.put((self.handler_id, req_id, op, key, value))
            serializer_start = time.time()
            while True:
                if self.result_pipe.poll(1):
                    resp = self.result_pipe.recv()
                    break
                else:
                    gevent.sleep(0.005)
            num_start = time.time()
            log_file.write(f"Pool {self.pool_id} Handler {self.handler_id} received response from serializer, time:{num_start-serializer_start:.3f} \n")
            while time.time() - num_start < 1:
                np.linalg.svd(np.random.rand(500, 500))
            log_file.write(f"Pool {self.pool_id} Handler {self.handler_id} processed request {req_id} , time taken: {time.time() - start:.3f}s\n")
            log_file.flush()

dispatch_interval = 0.005  # 200 qps at most
# --- 进程池 ---
class HandlerPool:
    def __init__(self, pool_id, num_handlers):
        self.pool_id = pool_id
        self.num_handlers = num_handlers
        self.task_queue = gevent.queue.Queue()
        self.serializer_req = Queue()
        self.handler_queues = []
        self.result_pipes = []
        self.handlers = []
        for i in range(num_handlers):
            task_queue = Queue()
            parent_result, child_result = Pipe()
            p = HandlerProcess(pool_id, i, task_queue, self.serializer_req, child_result)
            p.start()
            self.handler_queues.append(task_queue)
            self.result_pipes.append(parent_result)
            self.handlers.append(p)
        self.serializer = SerializerProcess(self.serializer_req, [parent_result for parent_result in self.result_pipes])
        self.serializer.start()
        self.start = time.time()
        gevent.spawn_later(dispatch_interval, self._dispatch_loop)

    # dispatch_loop
    def _dispatch_loop(self):
        gevent.spawn_later(dispatch_interval, self._dispatch_loop)
        gevent.spawn(self.dispatch)
    
    def dispatch(self):
        if not self.task_queue.empty():
            # 优先分配给队列为空的处理者，否则分配给队列最短的处理者
            min_len = None
            min_idx = None
            for idx, q in enumerate(self.handler_queues):
                qsize = q.qsize()
                if qsize == 0:
                    min_idx = idx
                    break
                if min_len is None or qsize < min_len:
                    min_len = qsize
                    min_idx = idx
            req = self.task_queue.get()
            self.handler_queues[min_idx].put(req)
            print(f"Dispatched request {req[0]} to handler {min_idx} (queue size: {min_len}), time: {time.time() - self.start:.3f}s")

    def submit(self, req_id, op, key, value):
        self.task_queue.put((req_id, op, key, value))
        return req_id

# --- 多个进程池 ---
NUM_POOLS = 2
NUM_HANDLERS_PER_POOL = 2
handler_pools = [HandlerPool(i, NUM_HANDLERS_PER_POOL) for i in range(NUM_POOLS)]

@app.route('/test', methods=['POST'])
def test():
    data = request.get_json()
    pool_id = int(data.get("pool_id", 0)) % NUM_POOLS
    op = data.get("op", "set")
    key = data.get("key", "k")
    value = data.get("value", 1)
    req_id = str(uuid.uuid4())
    handler_pools[pool_id].submit(req_id, op, key, value)
    return jsonify({
        "status": "queued",
        "pool_id": pool_id,
        "req_id": req_id
    })

if __name__ == '__main__':
    from gevent.pywsgi import WSGIServer
    print("Running in gevent (协程)模式")
    server = WSGIServer(('0.0.0.0', 9000), app)
    server.serve_forever()