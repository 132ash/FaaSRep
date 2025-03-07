from gevent import monkey
monkey.patch_all()
import gevent
import requests
from typing import Dict
import sys
import gevent.lock
sys.path.append('../../config')
import config

class ValidationQueue:
    def __init__(self, batch_size):
        self.queue = []
        self.queue_lock = gevent.lock.BoundedSemaphore()
        self.batch_size = batch_size
        self.repairing_batch_table: Dict[str, Dict] = {}

    def append(self, transaction_id: str, workflow_name:str, read_set: Dict[str, Dict], write_set: Dict[str, int], function_pos: Dict[str, str], RYW_subjection:Dict[str, dict]):
        self.queue_lock.acquire()
        self.queue.append({'transaction_id': transaction_id, "workflow_name":workflow_name, 
                           'read_set': read_set, 'write_set': write_set, 'function_pos': function_pos, 'RYW_subjection': RYW_subjection})
        self.queue_lock.release()

    # transform the batch from a list of txs to a dict, for the convenience of validation.
    # readset and writeset are lists for locking in sequence, so they are not transformed.
    def transform_batch(self, batch):
        transformed_batch = {
            "batch_id": batch[0]["transaction_id"],
            "workflow_name": {},
            "read_set": {},
            "write_set": {},
            "RYW_subjection": {},
            "function_pos": {},
            "transaction_list":[]
        }
        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["workflow_name"][tx_id] = tx["workflow_name"]
            transformed_batch["read_set"][tx_id]=tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["RYW_subjection"][tx_id]=tx["RYW_subjection"]
            transformed_batch["function_pos"][tx_id] = tx["function_pos"]
            transformed_batch["transaction_list"].append(tx_id)
        return transformed_batch

    def send_validate_request(self):
        self.queue_lock.acquire()
        # if len(self.queue) == 0:
        # TEST: batch size is fixed to 2
        if len(self.queue) != 2:
            self.queue_lock.release()
            return
        idx = min(self.batch_size, len(self.queue))
        batch = self.transform_batch(self.queue[:idx])
        self.queue = self.queue[idx:]
        self.queue_lock.release()
        self.repairing_batch_table[batch["batch_id"]] = {"batch_size": idx, "finished": 0, "lock": gevent.lock.BoundedSemaphore()}
        remote_url = 'http://{}/validate'.format(config.VALIDATOR_ADDR)
        data = {
            "batch": batch,
            "batch_id": batch["batch_id"]
        }
        response = requests.post(remote_url, json=data)
        response.close()

    def send_fin_repair_request(self, batch_id):
        self.repairing_batch_table[batch_id]['lock'].acquire()
        self.repairing_batch_table[batch_id]["finished"] += 1 
        self.repairing_batch_table[batch_id]['lock'].release()
        total = self.repairing_batch_table[batch_id]["batch_size"]
        finished = self.repairing_batch_table[batch_id]["finished"]
        if finished == total:
            remote_url = 'http://{}/fin_repair'.format(config.VALIDATOR_ADDR)
            data = {
                "batch_id": batch_id
            }
            response = requests.post(remote_url, json=data)
            response.close()
            self.repairing_batch_table.pop(batch_id, None)