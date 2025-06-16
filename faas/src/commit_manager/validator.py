from gevent import monkey, sleep, spawn
monkey.patch_all()
import gevent
from gevent import event
import sys
import logging
import time
from typing import Dict
from serializer import SerializerProcess, get_timestamp
import requests
from multiprocessing import Process, Queue, Pipe
from repair_info import RepairInfo
from repair_engine import RepairEngine
from validator_repo import Repository

sys.path.append('../../config')
import config
from collections import defaultdict

PESSIMISTIC_REPAIR_ENABLED = config.PESSIMISTIC_REPAIR and config.REPAIR


VALIDATE = 1
COMMIT = 2
CASCADED_COMMIT = 3
GATEWAY_ADDR = config.GATEWAY_ADDR
DISPATCH_INTERVAL = 0.005 

class ValidatorPool:
    def __init__(self, num_validators, workflow_name=None):
        self.num_validators = num_validators
        self.pool_task_queue = Queue()
        self.serializer_req_queue = Queue()
        self.handler_task_queues = []
        self.serializer_return_pipes = []
        self.validator_handlers = []
        self.workflow_name = workflow_name
        self.batch_processor_table = {}  # {batch_id: processor_id}
        for i in range(self.num_validators):
            task_queue = Queue()
            parent_put, child_get = Pipe()
            p = ValidatorProcess(i, workflow_name, task_queue, self.serializer_req_queue, child_get)
            p.start()
            self.handler_task_queues.append(task_queue)
            self.serializer_return_pipes.append(parent_put)
            self.validator_handlers.append(p)
        self.serializer = SerializerProcess(self.serializer_req_queue, [parent_put for parent_put in self.serializer_return_pipes], self.handler_task_queues)
        self.serializer.start()
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)

    def _dispatch_loop(self):
        gevent.spawn_later(DISPATCH_INTERVAL, self._dispatch_loop)
        gevent.spawn(self.dispatch)
    
    def dispatch(self):
        if not self.pool_task_queue.empty():
            req = self.pool_task_queue.get()
            batch_id = req[0]
            if batch_id in self.batch_processor_table:
                processor_id = self.batch_processor_table[batch_id]
                self.handler_task_queues[processor_id].put(req)
                logging.info(f"Dispatched request {req[0]} to handler {min_idx} (previously assigned processor)")
                return
            min_len = None
            min_idx = None
            for idx, q in enumerate(self.handler_task_queues):
                qsize = q.qsize()
                if min_len is None or qsize < min_len:
                    min_len = qsize
                    min_idx = idx
                    if min_len == 0:
                        break
            req = self.pool_task_queue.get()
            self.handler_task_queues[min_idx].put(req)
            logging.info(f"Dispatched request {req[0]} to handler {min_idx} (queue size: {min_len})")

    def submit(self, batch_id, op, data={}):
        self.pool_task_queue.put((batch_id, op, data))

class ValidatorProcess(Process):

    def __init__(self, validator_id, workflow_name, task_queue, serializer_req_queue, child_get, repo:Repository):
        super().__init__()
        self.validator_id = validator_id
        self.workflow_name = workflow_name
        self.repo = repo
        self.all_functions = self.repo.get_all_functions(workflow_name)
        self.function_info = self.repo.get_function_info(self.all_functions, workflow_name)
        self.tx_sink_addr =  self.function_info[self.repo.get_end_function(workflow_name)]['ip']
        self.task_queue = task_queue
        self.serializer_req_queue = serializer_req_queue
        self.serializer_return_pipe = child_get
        self.repair_info = RepairInfo(self.function_info)
        self.repair_engine = RepairEngine(self.repair_info, self.workflow_name, self.tx_sink_addr, repo)

        self.tx_list_per_batch = {}
        self.function_pos_per_batch = {} 
        self.worker_ip_set_per_batch = {}
        self.time_tuple_per_batch = {}  # {batch_id: (first_run_finish_time, last_task_time)}


    def run(self):
        while True:
            last_task_time = time.time()
            try:
                batch_id, op, data = self.task_queue.get(timeout=1)
                last_task_time = time.time()
            except:
                # 1秒无任务则休眠
                if time.time() - last_task_time > 1:
                    gevent.sleep(0.1)
                continue
            if op == VALIDATE:
                lock_set = data.get('lock_set', {})
                self.tx_list_per_batch[batch_id] = data['transaction_list']
                self.worker_ip_set_per_batch[batch_id] = data['worker_set']['transaction']
                batch_need_repair, expired_keys_per_ip, commit_list_for_current_handler, inside_validator_time, pessi_sink_info = self.validate(batch_id, data, last_task_time)
                self.time_tuple_per_batch[batch_id] = (data['first_run_finish_time'], last_task_time, inside_validator_time)
                if batch_need_repair:
                    self.repair_engine.repair_batch(batch_id, data, expired_keys_per_ip, pessi_sink_info)
                else:
                    self.commit_batch_list(commit_list_for_current_handler,  lock_set)
            elif op == COMMIT:
                ready_batch_list = self.serializer_request(batch_id, COMMIT, {})
                self.commit_batch_list(ready_batch_list)
            elif op == CASCADED_COMMIT:
                self.commit_batch_list(data)

    def serializer_request(self, batch_id, op, data):
        self.serializer_req_queue.put((self.validator_id, batch_id, op, data))
        while True:
            if self.serializer_return_pipe.poll(1):
                resp = self.serializer_return_pipe.recv()
                break
            else:
                gevent.sleep(0.005)
        return resp

    def validate(self, batch_id, batch, start_time):
        self.repair_info.batch_init(batch_id)
        Fake_version = get_timestamp()
        if config.BASIC or config.REMOTE_LOCK:
            return False, {}, [(batch_id, Fake_version)], time.time() - start_time
        else:
            serializer_input = {'function_pos':batch['function_pos'], 'transaction_list':batch['transaction_list'], 'read_set':batch['read_set'], 'write_set':batch['write_set']}
            batch_need_repair, expired_keys, subjection_set, commit_list_for_current_handler, pessi_sink_info = self.serializer_request(batch_id, VALIDATE, serializer_input)
            if not batch_need_repair or PESSIMISTIC_REPAIR_ENABLED:
                expired_keys_per_ip = {}
            else:
                expired_keys_per_ip = self.repair_info.construct_repair_metadata(batch_id, expired_keys, subjection_set, batch['RYW_subjection'], batch['function_pos'],  batch['worker_set']['batch'].keys(), batch['transaction_list'])
            return batch_need_repair, expired_keys_per_ip, commit_list_for_current_handler, time.time() - start_time, pessi_sink_info

    
    # commit_batch_list : [(batch_id, version), ...]
    def commit_batch_list(self, commit_batch_list, lock_set = {}):
        txid_lists, timestamps = [], []
        for batch_id, version in commit_batch_list:
            timestamps.append(self.time_tuple_per_batch[batch_id])
            txid_lists.append(self.tx_list_per_batch[batch_id])
            if config.REMOTE_LOCK:
                self.repo.sync_shadow_to_data_db_with_version(batch_id, version)
                self.repo.release_lock(batch_id, lock_set)
            else:
                worker_tx_set = defaultdict(list)
                for tx_id, worker_ip_list in self.worker_ip_set_per_batch[batch_id].items():
                    for ip in worker_ip_list:
                        worker_tx_set[ip].append(tx_id)
                jobs = [
                    gevent.spawn(self.trigger_worker_commit, batch_id, ip, version, worker_tx_set[ip])
                    for ip in worker_tx_set
                ]
                gevent.joinall(jobs)
            self.tx_list_per_batch.pop(batch_id, None)
            self.worker_ip_set_per_batch.pop(batch_id, None)
            self.time_tuple_per_batch.pop(batch_id, None)
            self.repair_info.clean_table_of_batch(batch_id, None)
        self.notify_gateway(txid_lists, True, timestamps)
                
        

    def trigger_worker_commit(self,batch_id, ip, version, tx_list):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        
        print(f"triggering batch_id {batch_id} commit, sending req to {ip}")
        data = {
            'batch_id':batch_id,
            "version": version,
            "tx_list": tx_list
        }
        requests.post(url, json=data)

    def notify_gateway(self, txid_lists, success:bool, timestamps):
        url = 'http://{}/notify'.format(GATEWAY_ADDR)
        data = {
            'transaction_id_lists': txid_lists,
            'success': success,
            'first_run_finish_time':timestamps
        }
        r = requests.post(url, json=data)
        return r.json() 
