import logging
import json
from gevent import monkey
monkey.patch_all()
import gevent.lock


import sys
from flask import Flask, request
import requests
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
        self.repairing_txs[transaction_id] = {'cond': gevent.lock.BoundedSemaphore(), 'finished': False, "repaired":False}
        self.repair_workflow(transaction_id, start_functions, workflow_name, expired_keys, function_pos)
        self.wait_Tx_repair_finish(transaction_id)
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

    def wait_Tx_repair_finish(self, tx_id):
        condition = self.repairing_txs[tx_id]['cond']
        while not self.repairing_txs[tx_id]['finished']:
            with condition:
                print(f"waiting for {tx_id} finish repair")
                condition.wait()

    def notify_Tx(self, tx_id, success):
        condition = self.repairing_txs[tx_id]['cond']
        self.repairing_txs[tx_id]['finished'] = True
        self.repairing_txs[tx_id]['repaired'] = success
        with condition:
            condition.notify_all()
        print(f"notified {tx_id}, repaired: {success}")

