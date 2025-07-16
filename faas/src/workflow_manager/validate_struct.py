from gevent import monkey
monkey.patch_all()
import gevent
from multiprocessing import Queue
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

PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR
VALIDATOR_ADDR = config.VALIDATOR_ADDR

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

class PessimisticBatchState:
    def __init__(self, batch_id, tx_list, batch_size):
        self.batch_size = batch_size
        self.batch_id = batch_id
        self.transaction_list = tx_list
        self.next_txs_after_batch = {} # {successor_batchid: [txid1, txid2, ...]}
        self.fin_consecutive_cnt = -1
        self.state_lock = gevent.lock.BoundedSemaphore()
        self.finished_tx_list = [None] * len(tx_list) 
        self.tx_idx = {txid: idx for idx, txid in enumerate(tx_list)}
        self.pessi_transaction_info = {txid:{'next_txs':[], 'prev_fin_cnt':2, 'fin_repair':False} for txid in tx_list}

    def trigger_successor(self, next_trigger_txs, ready_txs):
        for tx_id in next_trigger_txs:
            self.pessi_transaction_info[tx_id]['prev_fin_cnt'] -= 1
            if self.pessi_transaction_info[tx_id]['prev_fin_cnt'] == 0:
                ready_txs.append(tx_id)

    def init_tx_info(self, ready_txs, tx_sub, batch_successors):
        for tx_id in batch_successors:
            ready_txs.pop(tx_id, None)
            self.pessi_transaction_info[tx_id]['prev_fin_cnt'] -= 1
        for prev_tx_id, next_txs in tx_sub.items():
            self.pessi_transaction_info[prev_tx_id]['next_txs'] = next_txs
            for tx_id in next_txs:
                ready_txs.pop(tx_id, None)
                self.pessi_transaction_info[tx_id]['prev_fin_cnt'] -= 1

    def modify_batch_successors(self, next_batch_id, next_batch_txs, batch_successors):
        batch_successors.extend(next_batch_txs)
        self.state_lock.acquire()
        self.next_txs_after_batch.setdefault(next_batch_id, []).extend(next_batch_txs)
        self.state_lock.release()

    def transaction_finish(self, tx_id, ready_txs):
        txs_to_be_triggered_by_prev_finish = []
        finish_idx = self.tx_idx[tx_id]
        self.state_lock.acquire()
        self.finished_tx_list[self.tx_idx[tx_id]] = True
        if self.fin_consecutive_cnt == finish_idx - 1:
            while self.fin_consecutive_cnt < self.batch_size - 1 and self.finished_tx_list[self.fin_consecutive_cnt + 1] is not None:
                self.fin_consecutive_cnt += 1
                current_fin_tx_id = self.transaction_list[self.fin_consecutive_cnt]
                txs_to_be_triggered_by_prev_finish.extend(self.pessi_transaction_info[current_fin_tx_id]['next_txs'])
        self.trigger_successor(txs_to_be_triggered_by_prev_finish, ready_txs)
        self.state_lock.release()


class RepairingBatchState:
    def __init__(self, workflow_name):
        self.pessimistic_state_per_batch:Dict[str, PessimisticBatchState] = {}
        self.transaction_list_per_batch = {}
        self.tx_finished_table_per_batch = {}
        self.pessimistic_state_lock = gevent.lock.BoundedSemaphore()
        self.workflow_name = workflow_name

    def register_batch(self, batch_id, tx_list, batch_size):
        self.transaction_list_per_batch[batch_id] = tx_list
        self.tx_finished_table_per_batch[batch_id] = {'total':batch_size, "finished": 0, "lock": gevent.lock.BoundedSemaphore()}     
        if PESSIMISTIC_REPAIR:
            self.pessimistic_state_per_batch[batch_id] = PessimisticBatchState(batch_id, tx_list, batch_size)
            
    def update_pessimistic_subjection_info(self, batch_id:str, batch_sub, tx_sub):
        """
        Update the subjection info for the given batch. 
        batch_sub: {prev_batch_id:[txs]}
        tx_sub: {prev_batch_id: [txs]}
        """
        ready_txs = {txid: True for txid in self.transaction_list_per_batch[batch_id]}
        batch_successors = []
        for prev_batch_id, next_txs in batch_sub.items():
            self.pessimistic_state_lock.acquire()
            prev_batch_info = self.pessimistic_state_per_batch.get(prev_batch_id, None)
            if prev_batch_info:
                self.pessimistic_state_per_batch[batch_id].modify_batch_successors(batch_id, next_txs, batch_successors)
            self.pessimistic_state_lock.release()
        self.pessimistic_state_per_batch[batch_id].init_tx_info(ready_txs, tx_sub, batch_successors)
        return list(ready_txs.keys())

    def reminder_successor_tx_pessi(self, batch_id, tx_id, batch_finished):
        ready_txs = []
        batch_trigger_txs = []
        if batch_finished:
            self.pessimistic_state_lock.acquire()
            batch_trigger_txs = self.pessimistic_state_per_batch[batch_id].next_txs_after_batch
            for next_batch_id, next_trigger_txs in batch_trigger_txs.items():
                self.pessimistic_state_per_batch[next_batch_id].state_lock.acquire()
                self.pessimistic_state_per_batch[next_batch_id].trigger_successor(next_trigger_txs, ready_txs)
                self.pessimistic_state_per_batch[next_batch_id].state_lock.release()
            self.tx_finished_table_per_batch.pop(batch_id)
            self.pessimistic_state_lock.release()
        else:
            self.pessimistic_state_per_batch[batch_id].transaction_finish(tx_id, ready_txs)
        return ready_txs 
       
    def after_transaction_finish(self, batch_id, tx_id):
        self.tx_finished_table_per_batch[batch_id]['lock'].acquire()
        self.tx_finished_table_per_batch[batch_id]["finished"] += 1
        batch_finished = (self.tx_finished_table_per_batch[batch_id]["total"] == self.tx_finished_table_per_batch[batch_id]["finished"])
        self.tx_finished_table_per_batch[batch_id]['lock'].release()
        ready_successor_tx = []
        if PESSIMISTIC_REPAIR:
            ready_successor_tx = self.reminder_successor_tx_pessi(batch_id, tx_id, batch_finished)
        if batch_finished:
            self.tx_finished_table_per_batch.pop(batch_id, None)
        return batch_finished, ready_successor_tx

    
class TransactionSink:
    def __init__(self, workflow_name, batch_size, host_addr, repo: Repository):
        self.queue = []
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.start_functions = repo.get_start_functions(self.workflow_name + '_workflow_metadata')
        self.queue_lock = gevent.lock.BoundedSemaphore()
        self.batch_size = batch_size
        self.repairing_batch_state:RepairingBatchState = RepairingBatchState(workflow_name) 


    def append(self, transaction_id: str, read_set: Dict[str, Dict], write_set: Dict[str, int], container_port: Dict[str, str], RYW_subjection:Dict[str, dict]):
        self.queue_lock.acquire()
        self.queue.append({'transaction_id': transaction_id,
                           'read_set': read_set, 'write_set': write_set, 
                           'container_port': container_port, 
                           'RYW_subjection': RYW_subjection})
        self.queue_lock.release()

    # transform the batch from a list of txs to a dict, for the convenience of validation.
    # readset and writeset are lists for locking in sequence, so they are not transformed.
    def transform_batch(self, batch):
        transformed_batch = {
            "batch_id": batch[0]["transaction_id"],
            "read_set": {},
            "write_set": {},
            "RYW_subjection": {},
            "container_port": {},
            "transaction_list":[]
        }

        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["read_set"][tx_id]= tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["RYW_subjection"][tx_id] = tx["RYW_subjection"]
            transformed_batch["container_port"][tx_id] = tx["container_port"]
            transformed_batch["transaction_list"].append(tx_id)
        return transformed_batch

    def validate_batch(self):
        self.queue_lock.acquire()
        idx = min(self.batch_size, len(self.queue))
        # MODIFY: must wait the batch to finish: if batch open, wait until the batch is full.
        if idx != self.batch_size:
            self.queue_lock.release()
            return
        first_run_finish_time = time.time()
        batch = self.queue[:idx]
        self.queue = self.queue[idx:]
        self.queue_lock.release()
        batch = self.transform_batch(batch)
        self.repairing_batch_state.register_batch(batch['batch_id'], batch['transaction_list'],  idx)
        self.send_validate_request(batch, first_run_finish_time)

    def send_cascaded_repair_request_pessi(self, batch_id, tx_id, state, ready_txs):
        remote_url = 'http://{}/pessi_fin'.format(VALIDATOR_ADDR)
        data = {
            "workflow_name": self.workflow_name,
            "batch_id": batch_id,
            "tx_id": tx_id,
            "state": state,
            "ready_txs": ready_txs,
            'container_port': {},
        }
        requests.post(remote_url, json=data)
        
    def send_validate_request(self, batch, first_run_finish_time):
        remote_url = 'http://{}/validate'.format(VALIDATOR_ADDR)
        data = {
            'workflow_name': self.workflow_name,
            "batch": batch,
            "batch_id": batch["batch_id"],
            "first_run_finish_time": first_run_finish_time
        }
        logging.info(f"[VALIDATE] batch_id:{batch['batch_id']}, data:{data}")
        response = requests.post(remote_url, json=data)
        response.close() 
        

    def commit_batch(self, batch_id):
        remote_url = 'http://{}/commit'.format(VALIDATOR_ADDR)
        data = {
                'workflow_name': self.workflow_name,
                "batch_id": batch_id
            }
        requests.post(remote_url, json=data)

    def fin_repair_or_abort(self, batch_id, transaction_id, state):
        trigger_jobs = []
        batch_finished, ready_successors = self.repairing_batch_state.after_transaction_finish(batch_id, transaction_id)
        if PESSIMISTIC_REPAIR:
            trigger_jobs.append(gevent.spawn(self.send_cascaded_repair_request_pessi,batch_id, transaction_id, state, ready_successors))
        if batch_finished:
            trigger_jobs.append(gevent.spawn(self.commit_batch, batch_id))
        gevent.joinall(trigger_jobs)

    # called only in pessimistic repair, to update the subjection info of the batch.
    def register_pessimistic_info(self, batch_id, batch_sub, tx_sub):
        return {'ready_txs':self.repairing_batch_state.update_pessimistic_subjection_info(batch_id, batch_sub, tx_sub)}
        