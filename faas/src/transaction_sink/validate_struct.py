from gevent import monkey
monkey.patch_all()
import gevent
from multiprocessing import Queue
import requests
import logging
from typing import Dict
import sys
import time
from batch_state_struct import PessimisticBatchState, OptimisticTransactionState
import gevent.lock
sys.path.append('../../config')
import config

REPAIRED = config.REPAIRED
ABORTED = config.ABORTED    
WAITING = config.RUNNING


PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR
VALIDATOR_ADDR = config.VALIDATOR_ADDR


class RepairingBatchState:
    def __init__(self, workflow_name):
        self.pessimistic_state_per_batch:Dict[str, PessimisticBatchState] = {}
        self.optimistic_state_per_transaction:Dict[str, OptimisticTransactionState] = {}
        self.subjection_between_transactions = {} # {prev_batch_id: {next_batch_id: [txs]}}
        self.transaction_list_per_batch = {}
        self.tx_finished_table_per_batch = {}
        self.state_lock  = gevent.lock.BoundedSemaphore()
        self.workflow_name = workflow_name

    def register_batch(self, batch_id, tx_list, batch_size):
        self.transaction_list_per_batch[batch_id] = tx_list
        self.tx_finished_table_per_batch[batch_id] = {'total':batch_size, "finished": 0, "lock": gevent.lock.BoundedSemaphore()}     
        self.pessimistic_state_per_batch[batch_id] = PessimisticBatchState(batch_id, tx_list, batch_size)
        for tx_id in tx_list:
            self.optimistic_state_per_transaction[tx_id] = OptimisticTransactionState(batch_id, tx_id)
        logging.info(f"[REPAIR REGISTER] tx_finished_table_per_batch: {self.tx_finished_table_per_batch}")

    def update_subjection_info(self, batch_id:str, batch_sub, tx_sub, sub_per_tx_optimistic={}):
        """
        Update the subjection info for the given batch. 
        batch_sub: {prev_batch_id:[txs]}
        tx_sub: {prev_batch_id: [txs]}
        """
        ready_txs = {txid: True for txid in self.transaction_list_per_batch[batch_id]}
        batch_successors = []
        for prev_batch_id, next_txs in batch_sub.items():
            prev_batch_info = self.pessimistic_state_per_batch.get(prev_batch_id, None)
            if prev_batch_info:
                self.pessimistic_state_per_batch[prev_batch_id].modify_batch_successors(batch_id, next_txs, batch_successors)
        self.pessimistic_state_per_batch[batch_id].init_tx_info(ready_txs, tx_sub, batch_successors)
        # update optimistic subjection
        txs_need_pessimistic_repair = []
        for prev_transaction_id, next_txs in sub_per_tx_optimistic.items():
            next_txs = list(next_txs.keys())
            prev_state = self.optimistic_state_per_transaction[prev_transaction_id].modify_transaction_subjection(next_txs)
            if prev_state == ABORTED:
                txs_need_pessimistic_repair.extend(next_txs)
        for tx in txs_need_pessimistic_repair:
            self.optimistic_state_per_transaction[tx].need_pessimistic_repair = True
        return list(ready_txs.keys())

    def reminder_successor_tx_pessi(self, batch_id, tx_id, batch_finished):
        ready_txs = {}
        if batch_finished:
            batch_trigger_txs = self.pessimistic_state_per_batch[batch_id].next_txs_after_batch
            for next_batch_id, next_trigger_txs in batch_trigger_txs.items():
                self.pessimistic_state_per_batch[next_batch_id].trigger_successor(next_trigger_txs, ready_txs)
        else:
            self.pessimistic_state_per_batch[batch_id].transaction_finish(tx_id, ready_txs)
        logging.info(f"[PESSIMISTIC REMINDER NEXT] batch_id: {batch_id}, tx_id: {tx_id}, ready_txs: {ready_txs}")
        return ready_txs 
       
    def after_transaction_finish(self, batch_id, repair_mode, tx_id, state):
        finished_txs_and_state = []
        tasks_tx_finish_repair = {} # {batch_id: {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]}}
        tasks_cascaded_repair = {}
        if not PESSIMISTIC_REPAIR:
            rejected, successors_to_be_pessimistic = self.optimistic_state_per_transaction[tx_id].optimistic_state_change_after_repair(repair_mode, state)
            if rejected:
                logging.info(f"[OPTIMISTIC REPAIR REJECTED] Transaction {tx_id} in batch {batch_id} is rejected, state: {state}")
                return False, []
            if state == ABORTED:
                for tx_id in successors_to_be_pessimistic:
                    if not self.optimistic_state_per_transaction[tx_id].need_pessimistic_repair and self.optimistic_state_per_transaction[tx_id].optimistic_repair_state != ABORTED:
                        self.optimistic_state_per_transaction[tx_id].optimistic_repair_state = WAITING
                        self.optimistic_state_per_transaction[tx_id].need_pessimistic_repair = True
            if self.pessimistic_state_per_batch[batch_id].pessimistic_repair_ready[tx_id]:
                finished_txs_and_state = [(batch_id, tx_id, state)]
        else:
            finished_txs_and_state = [(batch_id, tx_id, state)]
        logging.info(f"[TX FINISH REQUEST] batch_id: {batch_id}, tx_id: {tx_id}, state: {state}, finished_txs_and_state: {finished_txs_and_state}")
        
        while finished_txs_and_state:
            batch_id , tx_id, state = finished_txs_and_state.pop(0)
            logging.info(f"[TX FINISH] batch_id: {batch_id}, tx_id: {tx_id}, finished_table: {self.tx_finished_table_per_batch[batch_id]}, state: {state}")
            self.tx_finished_table_per_batch[batch_id]["finished"] += 1
            batch_finished = (self.tx_finished_table_per_batch[batch_id]["total"] == self.tx_finished_table_per_batch[batch_id]["finished"])
            ready_successor_tx_pessi = self.reminder_successor_tx_pessi(batch_id, tx_id, batch_finished)
            if state == ABORTED:
                tasks_cascaded_repair.setdefault(batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['aborted_txs'].append(tx_id)
            if not PESSIMISTIC_REPAIR:
                for next_batch_id, tx_ids in ready_successor_tx_pessi.items():
                    for pessi_ready_tx in tx_ids:      
                        logging.info(f"[PESSIMISTIC CHECK NEXT] next_batch_id: {next_batch_id}, pessi_ready_tx: {pessi_ready_tx}, state:{self.optimistic_state_per_transaction[pessi_ready_tx].optimistic_repair_state}")           
                        optimistic_state = self.optimistic_state_per_transaction[pessi_ready_tx]
                        if optimistic_state.need_pessimistic_repair:
                            tasks_cascaded_repair.setdefault(next_batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['pessi_repair_txs'].append(pessi_ready_tx)
                        elif optimistic_state.optimistic_repair_state != WAITING:
                            finished_txs_and_state.append((next_batch_id, pessi_ready_tx, optimistic_state.optimistic_repair_state))
            else:
                for next_batch_id, tx_ids in ready_successor_tx_pessi.items():
                    tasks_cascaded_repair.setdefault(next_batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['pessi_repair_txs'].extend(tx_ids)

            if batch_finished:
                tasks_cascaded_repair.setdefault(batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['batch_finished'] = True
                tasks_tx_finish_repair[batch_id] = tasks_cascaded_repair.pop(batch_id)
                self.tx_finished_table_per_batch.pop(batch_id, None)
                self.pessimistic_state_per_batch.pop(batch_id, None)
                for tx_id in self.transaction_list_per_batch[batch_id]:
                    self.optimistic_state_per_transaction.pop(tx_id, None)
                self.transaction_list_per_batch.pop(batch_id, None)
        logging.info(f"[REPAIR FINISH] tasks_tx_finish_repair: {tasks_tx_finish_repair}, tasks_cascaded_repair: {tasks_cascaded_repair}")
        return tasks_tx_finish_repair, tasks_cascaded_repair

    
class TransactionSink:
    def __init__(self, workflow_name, batch_size, host_addr):
        self.queue = []
        self.host_addr = host_addr
        self.workflow_name = workflow_name
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


    def fin_repair_or_abort(self, batch_id, transaction_id, repair_mode, state):
        tasks_tx_finish_repair, tasks_cascaded_repair = self.repairing_batch_state.after_transaction_finish(batch_id, repair_mode, transaction_id, state)
        if tasks_tx_finish_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_tx_finish_repair)]
            gevent.joinall(tasks)
        if tasks_cascaded_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_cascaded_repair)]
            gevent.joinall(tasks)


    # called only in pessimistic repair, to update the subjection info of the batch.
    def register_repair_info_after_validate(self, batch_id, batch_sub, tx_sub, sub_per_tx):
        logging.info(f"[PESSIMISTIC REGISTER] batch_id: {batch_id}, batch_sub: {batch_sub}, tx_sub: {tx_sub}, sub_per_tx_optimistic: {sub_per_tx}")
        ready_txs = self.repairing_batch_state.update_subjection_info(batch_id, batch_sub, tx_sub, sub_per_tx)
        return {'ready_txs': ready_txs}
    
    def repair_finish_on_validator(self, data):
        remote_url = 'http://{}/fin_repair'.format(VALIDATOR_ADDR)
        requests.post(remote_url, json={'workflow_name':self.workflow_name, 'data':data})

    def send_validate_request(self, batch, first_run_finish_time):
        remote_url = 'http://{}/validate'.format(VALIDATOR_ADDR)
        data = {
            'workflow_name': self.workflow_name,
            "batch": batch,
            "batch_id": batch["batch_id"],
            "first_run_finish_time": first_run_finish_time
        }
        logging.info(f"[VALIDATE] batch_id:{batch['batch_id']}")
        response = requests.post(remote_url, json=data)
        response.close() 
        

    def commit_batch(self, batch_id):
        remote_url = 'http://{}/commit'.format(VALIDATOR_ADDR)
        data = {
                'workflow_name': self.workflow_name,
                "batch_id": batch_id
            }
        requests.post(remote_url, json=data)
        