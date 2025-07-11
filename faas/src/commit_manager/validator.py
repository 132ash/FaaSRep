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

PESSIMISTIC_REPAIR_ENABLED = not config.OPTIMISTIC_REPAIR


VALIDATE = 1
COMMIT = 2
CASCADED_COMMIT = 3
PESSIMISTIC_REPAIR_FINISH = 4
GATEWAY_ADDR = config.GATEWAY_ADDR
DISPATCH_INTERVAL = 0.005 

repo = Repository()

class ValidatorPool:
    def __init__(self, num_validators, workflow_name=None):
        self.num_validators = num_validators
        self.pool_task_queue = Queue()
        self.serializer_req_queue = Queue()
        self.handler_task_queues = []
        self.serializer_return_pipes = []
        self.validator_handlers = []
        function_pos = {}
        workflow_graph_topo = {}
        worker_ip_set = set()
        self.workflow_name = workflow_name
        function_info = repo.get_function_info(repo.get_all_functions(workflow_name), workflow_name)
        for func, info in function_info.items():
            function_pos[func] = info['ip']
            workflow_graph_topo[func] = info['next']
            worker_ip_set.add(info['ip'])
        worker_ip_set = list(worker_ip_set)
        self.batch_processor_table = {}  # {batch_id: processor_id}
        for i in range(self.num_validators):
            task_queue = Queue()
            parent_put, child_get = Pipe()
            p = ValidatorProcess(i, workflow_name, task_queue, self.serializer_req_queue, child_get, function_pos, workflow_graph_topo, worker_ip_set, repo)
            p.start()
            self.handler_task_queues.append(task_queue)
            self.serializer_return_pipes.append(parent_put)
            self.validator_handlers.append(p)
        self.serializer = SerializerProcess(self.serializer_req_queue, [parent_put for parent_put in self.serializer_return_pipes], self.handler_task_queues, function_pos)
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

    def __init__(self, validator_id, workflow_name, task_queue, serializer_req_queue, child_get, function_pos, workflow_graph_topo, worker_ip_set, repo:Repository):
        super().__init__()
        self.validator_id = validator_id
        self.workflow_name = workflow_name


        self.function_pos = function_pos
        self.workflow_graph_topo = workflow_graph_topo
        self.worker_ip_set = worker_ip_set
        self.repo = repo

        self.tx_sink_addr =  self.function_pos[self.repo.get_end_function(workflow_name)]
        self.task_queue = task_queue
        self.serializer_req_queue = serializer_req_queue
        self.serializer_return_pipe = child_get
        self.repair_info = RepairInfo(self.workflow_graph_topo,  self.function_pos)
        self.repair_engine = RepairEngine(self.repair_info, self.function_pos, self.worker_ip_set, self.workflow_name, self.tx_sink_addr, self.repo)

        self.tx_list_per_batch = {}
        self.container_port_per_batch = {} 
        self.read_set_per_batch = {}
        self.write_set_per_batch = {}
        self.successed_tx_list_per_batch = {}  # {batch_id: [tx_id1, tx_id2, ...]}
        self.time_tuple_per_batch = {}  # {batch_id: (first_run_finish_time, last_task_time)}


    def run(self):
        while True:
            last_task_time = time.time()
            try:
                batch_id, op, data = self.task_queue.get(timeout=1)
                last_task_time = time.time()
            except:
                if time.time() - last_task_time > 1:
                    gevent.sleep(0.1)
                continue
            if op == VALIDATE:
                lock_set = data.get('lock_set', {})
                self.tx_list_per_batch[batch_id] = data['transaction_list']
                self.successed_tx_list_per_batch[batch_id] = []
                self.read_set_per_batch[batch_id] = data['read_set']
                self.write_set_per_batch[batch_id] = data['write_set']
                batch_need_repair, expired_keys_per_ip, commit_list_for_current_handler, inside_validator_time, pessi_sink_info = self.validate(batch_id, data, last_task_time)
                self.time_tuple_per_batch[batch_id] = (data['first_run_finish_time'], last_task_time, inside_validator_time)
                if batch_need_repair:
                    self.repair_engine.repair_batch(batch_id, data['container_port'], self.write_set_per_batch[batch_id], self.tx_list_per_batch[batch_id], expired_keys_per_ip, pessi_sink_info)
                else:
                    self.commit_batch_list(commit_list_for_current_handler,  lock_set)
            elif op == COMMIT:
                ready_batch_list = self.serializer_request(batch_id, COMMIT, {})
                self.commit_batch_list(ready_batch_list)
            elif op == CASCADED_COMMIT:
                self.commit_batch_list(data)
            elif op == PESSIMISTIC_REPAIR_FINISH:
                self.repair_engine.pessimistic_repair_finish(batch_id, self.function_pos, self.worker_ip_set, self.write_set_per_batch[batch_id],self.successed_tx_list_per_batch[batch_id] , data)

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
        serializer_input = {'transaction_list':batch['transaction_list'], 'read_set':batch['read_set'], 'write_set':batch['write_set']}
        batch_need_repair, expired_keys, subjection_set, commit_list_for_current_handler, pessi_sink_info = self.serializer_request(batch_id, VALIDATE, serializer_input)
        if not batch_need_repair:
            expired_keys_per_ip = {}
        else:
            expired_keys_per_ip = self.repair_info.construct_repair_metadata(batch_id, expired_keys, subjection_set, batch['RYW_subjection'], self.worker_ip_set, batch['transaction_list'], batch['container_port'])
        return batch_need_repair, expired_keys_per_ip, commit_list_for_current_handler, time.time() - start_time, pessi_sink_info


    # commit_batch_list : [(batch_id, version), ...]
    def commit_batch_list(self, commit_batch_list, lock_set = {}):
        txid_lists, timestamps = [], []
        worker_commit_set = {worker_ip:{"txs":[]} for worker_ip in self.worker_ip_set }
        for batch_id, version, keys_for_commit_per_ip in commit_batch_list:
            timestamps.append(self.time_tuple_per_batch[batch_id])
            if PESSIMISTIC_REPAIR_ENABLED:
                successed_tx_list = self.successed_tx_list_per_batch[batch_id]
                keys_for_commit_per_ip = self.repair_engine.PessimisticRepairer.pessimistic_get_commit_keys_per_ip(batch_id)
            else:
                successed_tx_list = self.tx_list_per_batch[batch_id]
            txid_lists.append(successed_tx_list)
            for worker_ip in self.worker_ip_set:
                worker_commit_set[worker_ip]["txs"].extend(successed_tx_list)
                worker_commit_set[worker_ip]['keys'] = keys_for_commit_per_ip[worker_ip]
            self.tx_list_per_batch.pop(batch_id, None)
            self.time_tuple_per_batch.pop(batch_id, None)
            self.read_set_per_batch.pop(batch_id, None)
            self.write_set_per_batch.pop(batch_id, None)
            self.repair_engine.clean_table_of_batch(batch_id)
        jobs = [
            gevent.spawn(self.trigger_worker_commit, batch_id, ip, version, worker_commit_set[ip])
            for ip in worker_commit_set
        ]
        gevent.joinall(jobs)
        self.notify_gateway(txid_lists, True, timestamps)
                     

    def trigger_worker_commit(self,batch_id, ip, version, commit_info):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        
        print(f"triggering batch_id {batch_id} commit, sending req to {ip}")
        data = {
            'workflow_name': self.workflow_name,
            'batch_id':batch_id,
            "version": version,
            "tx_list": commit_info['txs'],
            'key_list': commit_info['keys']
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
