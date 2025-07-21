from gevent import monkey, sleep, spawn
monkey.patch_all()
import gevent
from gevent import event
import sys
import logging
import time
from subprocess_log import setup_validator_logger, log_validator_message
from serializer import SerializerProcess
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
PESSIMISTIC_CASCADED_REPAIR = 5
GATEWAY_ADDR = config.GATEWAY_ADDR
DISPATCH_INTERVAL = 0.005 
import re
Serializer_timeout = 10  # seconds
class ValidatorPool:
    def __init__(self, num_validators, workflow_name=None):
        self.num_validators = num_validators
        self.pool_task_queue = Queue()
        self.serializer_req_queue = Queue()
        self.assign_lock = gevent.lock.BoundedSemaphore()
        self.repo = Repository()
        self.handler_task_queues = []
        self.serializer_return_pipes = []
        self.workflow_name = workflow_name
        self.batch_processor_table = {}  # {batch_id: processor_id}
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
        self.serializer = SerializerProcess(self.serializer_req_queue, self.serializer_return_pipes, self.handler_task_queues, self.function_pos)
        self.serializer.daemon = True
        self.serializer.start()
        self.init()

    def init(self):
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
                logging.info(f"Dispatched request {req[0]} to handler {processor_id} (previously assigned processor)")
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
            self.batch_processor_table[batch_id] = min_idx
            self.handler_task_queues[min_idx].put(req)
            logging.info(f"Dispatched request {req[0]} to handler {min_idx} (queue size: {min_len})")

    def submit(self, batch_id, op, data={}):
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
        self.logger = setup_validator_logger(self.validator_id)
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
        self.tx_sink_addr = self.function_pos[self.repo.get_end_function(self.workflow_name)]
        self.repair_info = RepairInfo(self.logger, self.workflow_graph_topo,  self.function_pos)
        self.repair_engine = RepairEngine(self.logger, self.repair_info, self.function_pos, self.worker_ip_set, self.workflow_name, self.tx_sink_addr, self.repo)
        self.response_events = {} 
        self.register_lock = gevent.lock.BoundedSemaphore()
        self.response_lock = gevent.lock.BoundedSemaphore()
        self.tx_list_per_batch = {}
        self.container_port_per_batch = {} 
        self.read_set_per_batch = {}
        self.write_set_per_batch = {}
        self.successed_tx_list_per_batch = {}  # {batch_id: [tx_id1, tx_id2, ...]}
        self.time_tuple_per_batch = {}  # {batch_id: (first_run_finish_time, last_task_time)} 
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
        log_validator_message(self.logger, f"[{op}] processing batch {batch_id}") 
        last_task_time = time.time()
        if op == VALIDATE:
            batch = data['batch'] 
            self.register_lock.acquire()
            first_run_finish_time = data['first_run_finish_time']
            self.tx_list_per_batch[batch_id] = batch['transaction_list']
            self.successed_tx_list_per_batch[batch_id] = []
            self.read_set_per_batch[batch_id] = batch['read_set']
            self.write_set_per_batch[batch_id] = batch['write_set']
            self.container_port_per_batch[batch_id] = batch['container_port']
            batch_need_repair, expired_keys_per_ip, commit_list_for_current_handler, inside_validator_time, pessi_sink_info = self.validate(batch_id, batch, last_task_time)
            self.time_tuple_per_batch[batch_id] = (first_run_finish_time, last_task_time, inside_validator_time)
            self.register_lock.release()
            if batch_need_repair:
                self.repair_engine.repair_batch(batch_id, batch['container_port'], self.read_set_per_batch[batch_id], self.write_set_per_batch[batch_id], self.tx_list_per_batch[batch_id], expired_keys_per_ip, pessi_sink_info)
            else:
                self.commit_batch_list(commit_list_for_current_handler)
        elif op == COMMIT:
            ready_batch_list = self.serializer_request(batch_id, COMMIT, {})
            self.commit_batch_list(ready_batch_list)
        elif op == CASCADED_COMMIT:
            self.commit_batch_list(data)
        elif op == PESSIMISTIC_REPAIR_FINISH:
            self.repair_engine.pessimistic_repair_finish(batch_id, self.write_set_per_batch[batch_id],self.successed_tx_list_per_batch[batch_id] , self.container_port_per_batch[batch_id], data)
            if data.get('batch_finish', False):
                ready_batch_list = self.serializer_request(batch_id, COMMIT, {})
                self.commit_batch_list(ready_batch_list)
        elif op == PESSIMISTIC_CASCADED_REPAIR:
            self.repair_engine.send_pessimistic_repair_req(batch_id, self.container_port_per_batch[batch_id], data['ready_txs'])
            
    def serializer_request(self, batch_id, op, data):
        res_event = event.AsyncResult()
        self.response_lock.acquire()
        self.response_events[batch_id] = res_event
        self.response_lock.release()
        self.serializer_req_queue.put((self.validator_id, batch_id, op, data))
        serilizer_res = res_event.get(timeout=Serializer_timeout)
        return serilizer_res

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
    def commit_batch_list(self, commit_batch_list):
        txid_lists, timestamps = [], []
        worker_commit_set = {worker_ip:{'keys':[], 'txs':[]} for worker_ip in self.worker_ip_set }
        for batch_id, version, keys_for_commit_per_ip in commit_batch_list:
            timestamps.append(self.time_tuple_per_batch[batch_id])
            if PESSIMISTIC_REPAIR_ENABLED:
                successed_tx_list = self.successed_tx_list_per_batch[batch_id]
                keys_for_commit_per_ip = self.repair_engine.PessimisticRepairer.pessimistic_get_commit_keys_per_ip(batch_id)
            else:
                successed_tx_list = self.tx_list_per_batch[batch_id]
            txid_lists.append(successed_tx_list)
            for worker_ip in self.worker_ip_set:
                worker_commit_set[worker_ip]['txs'].extend(self.tx_list_per_batch[batch_id])
                worker_commit_set[worker_ip]['keys'].append({"keys": keys_for_commit_per_ip[worker_ip], 'version': version})
            self.tx_list_per_batch.pop(batch_id, None)
            self.time_tuple_per_batch.pop(batch_id, None)
            self.read_set_per_batch.pop(batch_id, None)
            self.write_set_per_batch.pop(batch_id, None)
            self.repair_engine.clean_table_of_batch(batch_id)
            self.container_port_per_batch.pop(batch_id, None)
        jobs = [
            gevent.spawn(self.trigger_worker_commit, ip, worker_commit_set[ip])
            for ip in worker_commit_set
        ]
        gevent.joinall(jobs)
        self.notify_gateway(txid_lists, True, timestamps)
                     

    def trigger_worker_commit(self, ip, commit_list):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        
        data = {
            'workflow_name': self.workflow_name,
            "commit_list": commit_list
        }

        requests.post(url, json=data)

    def notify_gateway(self, txid_lists, success:bool, timestamps):
        url = 'http://{}/notify'.format(GATEWAY_ADDR)
        data = {
            'transaction_id_lists': txid_lists,
            'success': success,
            'timestamps':timestamps
        }
        r = requests.post(url, json=data)
        return r.json() 
