import logging
import requests
from gevent import monkey
monkey.patch_all()
import gevent.lock


import sys
from flask import Flask, request
from gevent import event
import time
import validator_repo

sys.path.append('../../config')
import config

repo = validator_repo.Repository()

class RepairEngine:

    def __init__(self):
        self.repairing_batches = {}
        
    def trigger_repair(self, batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, dirty_set, downstream_func_table, upstream_func_table, RYW_subjection):
        self.repairing_batches[batch_id] = {'cond': event.Event(), 'finished': False, "repaired":False}
        self.repairing_batches[batch_id]['cond'].clear()
        self.repair_batch(batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, dirty_set, downstream_func_table, upstream_func_table, RYW_subjection)
        self.repairing_batches[batch_id]['cond'].wait()
        if self.repairing_batches[batch_id]['repaired']:
            return True
        else:
            return False

    def trigger_function(self, workflow_name, transaction_id, function_name, ip, expired_keys, dirty_set, downstream_func_table, upstream_func_table, batch_id, RYW_subjection):
        if not ip.endswith(":7000"):
            url = 'http://{}:7000/request'.format(ip)
        else:
            url = 'http://{}/request'.format(ip)
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'expired_keys': expired_keys,
            'dirty_set': dirty_set,
            'RYW_subjection':RYW_subjection,
            'downstream_func_table': downstream_func_table, 
            'upstream_func_table': upstream_func_table,
            'repair': True
        }
        print(f"triggering {function_name}, sending req to {url}, batch_id: {batch_id}")
        requests.post(url, json=data)

    def repair_batch(self,batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, dirty_set, downstream_func_table, upstream_func_table, RYW_subjection):
        # allocate works
        start = time.time()
        jobs = []
        for tx_id in transaction_list:
            workflow_name = workflow_name_per_tx[tx_id]
            start_functions = repo.get_start_functions(workflow_name + '_workflow_metadata')
            for n in start_functions:
                ip = function_pos_per_tx[tx_id][n]
                jobs.append(gevent.spawn(self.trigger_function, workflow_name, tx_id, n, ip, expired_keys, dirty_set.get(tx_id,{}), downstream_func_table.get(tx_id,{}), upstream_func_table.get(tx_id,{}), batch_id, RYW_subjection.get(tx_id,{})))
        gevent.joinall(jobs)   
        end = time.time()
        return end - start

    def notify_batch(self, batch_id, success):
        self.repairing_batches[batch_id]['repaired'] = success
        self.repairing_batches[batch_id]['cond'].set()
        print(f"notified {batch_id}, repaired: {success}")

