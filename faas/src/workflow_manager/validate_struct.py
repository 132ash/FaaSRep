from gevent import monkey
monkey.patch_all()
import gevent
import requests
import logging
from workersp_repo import Repository
from typing import Dict
import sys
import time
import gevent.lock
sys.path.append('../../config')
import config

REPAIRED = 1
ABORTED = 2
WAITING = 3

# reserve the containers after the first run, and return to the container pool after repairing.
class ReservePool:
    def __init__(self):
        self.pool = {}  # {transaction_id: {lock: xx, containers:[container1, container2, ...]}}
        self.queue_lock = gevent.lock.BoundedSemaphore()
        
    def reserve(self, transaction_id, container):
        self.queue_lock.acquire()
        if transaction_id not in self.pool:
            self.pool[transaction_id] = {"lock": gevent.lock.BoundedSemaphore(), "containers": []}
        self.queue_lock.release()
        self.pool[transaction_id]["lock"].acquire()
        self.pool[transaction_id]["containers"].append(container)
        self.pool[transaction_id]["lock"].release()

    def release(self, transaction_id):
        containers = self.pool.get(transaction_id, {}).get("containers", [])
        for container in containers:
            container.return_to_pool()
        self.pool.pop(transaction_id, None)

class RepairingBatchState:
    def __init__(self, workflow_name):
        self.batch_state = {}
        self.pessi_transaction_info = {}
        self.batch_subjection_table = {}
        self.workflow_name = workflow_name

    def release_batch_info(self, batch_id):
        self.batch_state.pop(batch_id, None)

    def register_batch(self, batch_id, batch, batch_size):
        tx_list = batch['tx_list']
        if config.OPTIMISTIC_REPAIR:
            self.batch_state[batch_id] = {'txs':tx_list, 'total':batch_size, "finished": 0, "lock": gevent.lock.BoundedSemaphore()}
        else:
            self.pessi_transaction_info[batch_id] = {txid:{'rs':{}, 'ws':{}, 'self_state':WAITING, 
                                                    'next_txs':[], 'up_cnt':0, 'fin_cnt':0, 
                                                    "tx_lock": gevent.lock.BoundedSemaphore()} for txid in tx_list}
            self.batch_state[batch_id] = {'aborted_tx':[], 'txs':tx_list,'total': batch_size, "finished": 0, 
                                          "lock": gevent.lock.BoundedSemaphore(), 'sub_table':{}}

    def reminder_successor_tx(self, batch_id, tx_id, trigger_jobs:list):
        for succsessor_batch_id, succsessor_txid in self.batch_state[batch_id]['transaction_info'][tx_id]['next_txs']:
            self.batch_state[succsessor_batch_id]['transaction_info'][succsessor_txid]['tx_lock'].acquire()
            self.batch_state[succsessor_batch_id]['transaction_info'][succsessor_txid]['fin_cnt'] += 1
            if self.batch_state[succsessor_batch_id]['transaction_info'][succsessor_txid]['fin_cnt'] == self.batch_state[succsessor_batch_id]['transaction_info'][succsessor_txid]['up_cnt']:
                trigger_jobs.append(gevent.spawn(self.start_repair, batch_id, succsessor_txid))
            self.batch_state[succsessor_batch_id]['transaction_info'][succsessor_txid]['tx_lock'].release()
                      
       
    def after_transaction_finish(self, batch_id, tx_id, state):
        trigger_jobs = []
        self.batch_state[batch_id]['transaction_info'][tx_id]['tx_lock'].acquire()
        self.reminder_successor_tx(batch_id, tx_id, trigger_jobs)
        self.batch_state[batch_id]['transaction_info'][tx_id]['self_state'] = state
        self.batch_state[batch_id]['transaction_info'][tx_id]['tx_lock'].release()

        batch_finished = False
        total = self.batch_state[batch_id]["total"]
        self.batch_state[batch_id]['lock'].acquire()
        self.batch_state[batch_id]["finished"] += 1
        if state == ABORTED:
            self.batch_state[batch_id]['aborted_tx'].append(tx_id)
            logging.info(f"transaction {tx_id} in batch {batch_id} aborted.")
        if total == self.batch_state[batch_id]["finished"]:
            self.batch_state[batch_id]['lock'].release()
            batch_finished = True
            self.batch_state.pop(batch_id, None)
            logging.info(f"batch {batch_id} finished, all txs repaired or aborted.")
        else:
            self.batch_state[batch_id]['lock'].release()
        return batch_finished, trigger_jobs


    def update_batch_info(self, batch_id, previous_subjection_info: Dict[str, dict], batch_subjection_info: Dict[str, dict]):
        ready_txs = []
        for txid, tx_info in batch_subjection_info.items():
            for key, info in tx_info.items():   
                self.pessi_transaction_info[batch_id]['transaction_info'][txid][key] = info # up_cnt, next_txs 

        # check previous subjection: the tx may be finished already, or the batch may be commited. 
        for prev_batch_id, prev_batch_info in previous_subjection_info.items():
            if prev_batch_id not in self.batch_state:
                for _, succsessors in prev_batch_info.items():
                    for succsessor_txid in succsessors:
                        self.batch_state[batch_id]['transaction_info'][succsessor_txid]['fin_cnt'] += 1 
            else:
                for prev_txid, succsessors in prev_batch_info.items():
                    self.batch_state[prev_batch_id]['transaction_info'][prev_txid]['tx_lock'].acquire()
                    if self.batch_state[prev_batch_id]['transaction_info'][prev_txid]['self_state'] != WAITING:
                        for succsessor_txid in succsessors:
                            self.batch_state[batch_id]['transaction_info'][succsessor_txid]['fin_cnt'] += 1
                            if self.batch_state[batch_id]['transaction_info'][succsessor_txid]['fin_cnt'] == self.batch_state[batch_id]['transaction_info'][succsessor_txid]['up_cnt']:
                                ready_txs.append(succsessor_txid)
                    else:
                        self.batch_state[prev_batch_id]['transaction_info'][prev_txid]['next_txs'].extend(succsessors)
                    self.batch_state[prev_batch_id]['transaction_info'][prev_txid]['tx_lock'].release()
        return ready_txs
    
    def start_repair(self, batch_id, tx_id):
        start_functions = self.batch_state[batch_id]['transaction_info'][tx_id]['start_functions']
        trigger_jobs = []
        for start_function in start_functions:
            ip = start_function['ip']
            port = start_function['port']
            name = start_function['name']
            trigger_jobs.append(gevent.spawn(self.trigger_start_function, batch_id, tx_id, name, ip, port))
        gevent.joinall(trigger_jobs)


    def trigger_start_function(self, batch_id, tx_id, function_name, ip, port):
        data = {
                'batch_id': batch_id,
                'transaction_id': tx_id,
                'workflow_name': self.workflow_name,
                'function_name': function_name,
                'no_parent_execution': True,
                'repair': True,
        }
        if config.FAST_PATH:
            route = 'repair'
            data['port'] = port
        else:
            route = 'request'
            data['repair_states'] = self.batch_state[batch_id]['transaction_info'][tx_id]['repair_states'] 
        if not ip.endswith(":7000"):
            url = f'http://{ip}:7000/{route}'
        else:
            url = f'http://{ip}/{route}'
        requests.post(url, json=data)
    
class TransactionSink:
    def __init__(self, workflow_name, batch_size, host_addr, repo: Repository):
        self.queue = []
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.start_functions = repo.get_start_functions(self.workflow_name + '_workflow_metadata')
        self.queue_lock = gevent.lock.BoundedSemaphore()
        self.batch_size = batch_size
        self.repairing_batch_state:RepairingBatchState= RepairingBatchState(workflow_name) 


    def append(self, transaction_id: str, workflow_name:str, read_set: Dict[str, Dict], write_set: Dict[str, int], function_pos: Dict[str, str], worker_set: Dict[str, str], RYW_subjection:Dict[str, dict], lock_set:Dict[str, bool]):
        self.queue_lock.acquire()
        self.queue.append({'transaction_id': transaction_id, "workflow_name":workflow_name, 'worker_set': worker_set,
                           'read_set': read_set, 'write_set': write_set, 'function_pos': function_pos, 'RYW_subjection': RYW_subjection, 'lock_set':lock_set})
        self.queue_lock.release()

    # transform the batch from a list of txs to a dict, for the convenience of validation.
    # readset and writeset are lists for locking in sequence, so they are not transformed.
    def transform_batch(self, batch):
        transformed_batch = {
            "batch_id": batch[0]["transaction_id"],
            "read_set": {},
            "write_set": {},
            "RYW_subjection": {},
            "function_pos": {},
            'worker_set':{'batch':{}, 'transaction':{}},
            "transaction_list":[],
            "lock_set": {},
            'sink_addr': self.host_addr 
        }
        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["read_set"][tx_id]=tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["RYW_subjection"][tx_id] = tx["RYW_subjection"]
            transformed_batch["function_pos"][tx_id] = tx["function_pos"]
            transformed_batch["worker_set"]['transaction'][tx_id] = tx["worker_set"].keys()
            transformed_batch["worker_set"]['batch'].update(tx["worker_set"])
            transformed_batch["transaction_list"].append(tx_id)
            transformed_batch["lock_set"][tx_id] = tx["lock_set"]
        return transformed_batch

    def send_validate_request(self):
        self.queue_lock.acquire()
        idx = min(self.batch_size, len(self.queue))
        # MODIFY: must wait the batch to finish: if batch open, wait until the batch is full.
        if (config.BATCH_SIZE == 1 and idx == 0) or (config.BATCH_SIZE != 1 and idx != config.BATCH_SIZE):
            self.queue_lock.release()
            return
        first_run_finish_time = time.time()
        batch = self.queue[:idx]
        self.queue = self.queue[idx:]
        self.queue_lock.release()
        batch = self.transform_batch(batch)
        self.repairing_batch_state.register_batch(batch['batch_id'], batch, idx)
        logging.info(f"send validate request: {batch['batch_id']}, all tx: {batch['transaction_list']}, batch_size:{idx}")
        remote_url = 'http://{}/validate'.format(config.VALIDATOR_ADDR)
        data = {
            "batch": batch,
            "batch_id": batch["batch_id"],
            "first_run_finish_time": first_run_finish_time
        }
        response = requests.post(remote_url, json=data)
        response.close()

    def send_fin_repair_request(self, batch_id):
        remote_url = 'http://{}/fin_repair'.format(config.VALIDATOR_ADDR)
        data = {
                'workflow_name': self.workflow_name,
                "batch_id": batch_id
            }
        requests.post(remote_url, json=data)

    def fin_repair(self, batch_id, transaction_id):
        batch_finished, trigger_jobs = self.repairing_batch_state.after_transaction_finish(batch_id, transaction_id, REPAIRED)
        if batch_finished:
            trigger_jobs.append(gevent.spawn(self.send_fin_repair_request, batch_id))
        gevent.joinall(trigger_jobs)

    def abort_during_repair(self, batch_id, transaction_id, trigger_jobs):
        batch_finished, trigger_jobs = self.repairing_batch_state.after_transaction_finish(batch_id, transaction_id, ABORTED)
        if batch_finished:
            trigger_jobs.append(gevent.spawn(self.send_fin_repair_request, batch_id))

    # called only in pessimistic repair, to update the subjection info of the batch.
    def repair_pessimistic(self, batch_id, prev_batch_info: Dict[str, dict], current_batch_info: Dict[str, dict]):
        ready_tx = self.repairing_batch_state.update_batch_info(batch_id, prev_batch_info, current_batch_info)
        ready_tx_jobs = []
        for tx_id in ready_tx:
            ready_tx_jobs.append(gevent.spawn(self.repairing_batch_state.start_repair, batch_id, tx_id))
        gevent.joinall(ready_tx_jobs)