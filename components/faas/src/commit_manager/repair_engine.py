import logging
import json
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
        self.repairing_txs = {}
        
    def trigger_repair(self, transaction_id, workflow_name, expired_keys, confilcted, function_pos):
        if not confilcted:
            return True
        start_functions = repo.get_start_functions(workflow_name + '_workflow_metadata')
        self.repairing_txs[transaction_id] = {'cond': event.Event(), 'finished': False, "repaired":False}
        self.repairing_txs[transaction_id]['cond'].clear()
        self.repair_workflow(transaction_id, start_functions, workflow_name, expired_keys, function_pos)
        self.repairing_txs[transaction_id]['cond'].wait()
        if self.repairing_txs[transaction_id]['repaired']:
            return True
        else:
            return False

    def trigger_function(self, workflow_name, transaction_id, function_name, ip, expired_keys):
        url = 'http://{}/request'.format(ip)
        data = {
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'expired_keys': expired_keys,
            'repair': True
        }
        print(f"triggering {function_name}, sending req to {url}")
        # requests.post(url, json=data)

    def repair_workflow(self, transaction_id, start_functions, workflow_name, expired_keys, function_pos):
        # allocate works
        logging.info(f"repairing {workflow_name}, ID:{transaction_id} with expired set {expired_keys}" )
        start = time.time()
        jobs = []
        for n in start_functions:
            ip = function_pos[n]
            jobs.append(gevent.spawn(self.trigger_function, workflow_name, transaction_id, n, ip, expired_keys))
        gevent.joinall(jobs)
        end = time.time()
        return end - start

    def notify_Tx(self, tx_id, success):
        self.repairing_txs[tx_id]['repaired'] = success
        self.repairing_txs[tx_id]['cond'].set()
        print(f"notified {tx_id}, repaired: {success}")

