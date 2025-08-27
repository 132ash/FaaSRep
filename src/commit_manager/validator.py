from gevent import monkey
monkey.patch_all()
import gevent
from gevent import event
import sys
import gevent.lock
import gevent.queue
import logging
from subprocess_log import log_message, setup_validator_logger
import time
from serializer import SerializerProcess
import requests
from multiprocessing import Process, Queue
from validator_repo import Repository

sys.path.append('../../config')
import config

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

VALIDATE = 1
REPAIR_FINISH = 2
COMMIT = 3
CASCADED_COMMIT = 4
GATEWAY_ADDR = config.GATEWAY_ADDR
CACHE_ENABLED = config.CACHE_ENABLED    
DISPATCH_INTERVAL = 0.005 
import re
Serializer_timeout = 10  # seconds


class ValidatorPool:
    def __init__(self, num_validators, workflow_name=None):
        self.num_validators = num_validators
        self.pool_task_queue = gevent.queue.Queue()
        self.serializer_req_queue = Queue()
        self.assign_lock = gevent.lock.BoundedSemaphore()
        self.repo = Repository()
        self.handler_task_queues = []
        self.serializer_return_pipes = []
        self.workflow_name = workflow_name
        self.function_pos = {}
        function_info = self.repo.get_function_info(self.repo.get_all_functions(self.workflow_name), self.workflow_name)
        for func, info in function_info.items():
            self.function_pos[func] = info['ip']

        for i in range(self.num_validators):
            task_queue = Queue()
            serializer_return_pipe = Queue()
            p = ValidatorProcess(i, workflow_name, task_queue, self.serializer_req_queue,  serializer_return_pipe)
            p.daemon = True
            p.start()
            self.handler_task_queues.append(task_queue)
            self.serializer_return_pipes.append(serializer_return_pipe)
        self.serializer = SerializerProcess(self.workflow_name, self.serializer_req_queue, self.serializer_return_pipes, self.handler_task_queues, self.function_pos)
        self.serializer.daemon = True
        self.serializer.start()
        self.init()

    def init(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)

    def _dispatch_loop(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn(self.dispatch)
    
    def dispatch(self):
        while not self.pool_task_queue.empty():
            req = self.pool_task_queue.get()
            batch_id = req[0]
            processor_id_to_assign = hash(batch_id) % self.num_validators
            self.handler_task_queues[processor_id_to_assign].put(req)

    def submit(self, batch_id, op, data={}):
        # logging.info(f"[{self.workflow_name}] submit batch {batch_id}.")
        self.pool_task_queue.put((batch_id, op, data))

class ValidatorProcess(Process):

    def __init__(self, validator_id, workflow_name, task_queue, serializer_req_queue, child_get):
        super().__init__()
        self.validator_id = validator_id
        self.workflow_name = workflow_name
        self.task_queue = task_queue
        self.serializer_req_queue = serializer_req_queue
        self.serializer_return_pipe = child_get

    def _dispatch_loop(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn(self.dispatch_serilizer_response)

    def dispatch_serilizer_response(self):
        if not self.serializer_return_pipe.empty():
            batch_id, data = self.serializer_return_pipe.get()
            if batch_id in self.response_events:
                return_event = self.response_events.pop(batch_id)
                return_event.set(data)


    def run(self):
        # 进程内初始化所有共享属性
        self.logger = setup_validator_logger(self.workflow_name, self.validator_id)
        self.repo = Repository()
        self.function_pos = {}
        self.workflow_graph_topo = {}
        self.worker_ip_set = set()
        function_info = self.repo.get_function_info(self.repo.get_all_functions(self.workflow_name), self.workflow_name)
        for func, info in function_info.items():
            self.function_pos[func] = info['ip']
            self.workflow_graph_topo[func] = info['next']
            self.worker_ip_set.add(info['ip'])
        self.worker_ip_set = list(self.worker_ip_set)
        self.response_events = {} 
        self.register_lock = gevent.lock.BoundedSemaphore()
        self.response_lock = gevent.lock.BoundedSemaphore()
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        last_task_time = time.time()
        while True:
            try:
                batch_id, op, data = self.task_queue.get(timeout=1)
                gevent.spawn(self.handle_task, batch_id, op, data)
                last_task_time = time.time()
            except:
                if time.time() - last_task_time > 1:
                    gevent.sleep(0.1)

    def handle_task(self, batch_id, op, data):
        last_task_time = time.time()
        if op == VALIDATE:
            batch = data['batch'] 
            inside_validator_time, version, commitable_keys, expired_set, succeeded_txs, abort_txs = self.validate(batch_id, batch, last_task_time)
            commit_time = self.commit_tx_list(version, commitable_keys, expired_set)
            self.notify_gateway(succeeded_txs, [inside_validator_time, commit_time], abort_txs)

                        
    def serializer_request(self, batch_id, op, data):
        res_event = event.AsyncResult()
        self.response_lock.acquire()
        self.response_events[batch_id] = res_event
        self.response_lock.release()
        self.serializer_req_queue.put((self.validator_id, batch_id, op, data))
        serilizer_res = res_event.get(timeout=Serializer_timeout)
        return serilizer_res

    def validate(self, batch_id, batch, start_time):
        serializer_input = {'transaction_list':batch['transaction_list'], 'read_set':batch['read_set'], 'write_set':batch['write_set']}
        version, commitable_keys,expired_set, succeeded_txs, abort_txs = self.serializer_request(batch_id, VALIDATE, serializer_input)
        return time.time() - start_time,  version, commitable_keys, expired_set, succeeded_txs, abort_txs

    def commit_tx_list(self, version, commitable_keys, expired_set):
        start = time.time()
        if CACHE_ENABLED:
            worker_commit_set = {worker_ip:{'commit_keys':[], 'expired_keys':[]} for worker_ip in self.worker_ip_set}
            for key, expired_func_info in expired_set.items():
                for expired_func in expired_func_info:
                    worker_commit_set[self.function_pos[expired_func]]['expired_keys'].append(key)
            for key, key_info in commitable_keys.items():
                txid = key_info[0]
                func = key_info[1]
                worker_commit_set[self.function_pos[func]]['commit_keys'].append([f'{txid}:PUT:{func}:{key}', version])
            #log_message(self.logger, f"[COMMIT] Commit batch list: {commit_batch_list}, txid_lists: {txid_lists}, aborted_txs:{abort_txs}")
            jobs = [
                gevent.spawn(self.trigger_worker_commit, ip, worker_commit_set[ip])
                for ip in worker_commit_set
            ]
            gevent.joinall(jobs)
        else:
            txs_to_commit = {}
            for key, key_info in commitable_keys.items():
                txid = key_info[0]
                func = key_info[1]
                if txid not in txs_to_commit:
                    txs_to_commit[txid] = []
                txs_to_commit[txid].append([key, func])
            commit_jobs = [
                gevent.spawn(self.repo.sync_shadow_to_data_db_with_version, txid, keys, version)
                for txid, keys in txs_to_commit.items()
            ]
            gevent.joinall(commit_jobs)
        return time.time() - start
            

    def trigger_worker_commit(self, ip, worker_commit_set):
        if not ip.endswith(":7500"):
            url = f"http://{ip}:7500/commit"
        else:
            url = f"http://{ip}/commit"
        
        data = {
            'workflow_name': self.workflow_name,
            "commit_keys": worker_commit_set['commit_keys'],
            'expired_keys': worker_commit_set['expired_keys']
        }

        requests.post(url, json=data)

    def notify_gateway(self, commited_txs, timestamps, aborted_txs):
        url = 'http://{}/notify'.format(GATEWAY_ADDR)
        data = {
            'commited_txs': commited_txs,
            'timestamps':timestamps,
            'aborted_txs': aborted_txs,
            'from_validator': True
        }
        r = requests.post(url, json=data)
        return r.json() 

