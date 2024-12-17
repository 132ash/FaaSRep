import json
import gevent.lock
from gevent import monkey
import uuid
monkey.patch_all()
import sys
from flask import Flask, request
import requests
import time
import logging

sys.path.append('../../config')
import config

class RepairEngine:

    def __init__(self, commit_manager):
        self.commit_manager = commit_manager
        self.repairing_txs = {}
        
    def trigger_repair(self, transaction_id, start_functions, workflow_name, expired_keys, confilcted, function_pos):
        if not confilcted:
            return True
        self.repairing_txs[transaction_id] = {'cond': gevent.lock.BoundedSemaphore(), 'finished': False, "repaired":False}
        self.repair_workflow(transaction_id, start_functions, workflow_name, expired_keys, function_pos)
        self.wait_Tx_repair_finish(transaction_id)
        if self.repairing_txs[transaction_id]['repaired']:
            return True
        else:
            return False

    def trigger_function(self, workflow_name, transaction_id, function_name, ip, expired_keys):
        url = 'http://{}/request'.format(ip)
        print(f"sending req to {url}")
        data = {
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'expired_keys': expired_keys,
            'repair': True
        }
        requests.post(url, json=data)

    def repair_workflow(self, transaction_id, start_functions, workflow_name, expired_keys, function_pos):
        # allocate works
        print(f"start_functions: {start_functions}")
        start = time.time()
        jobs = []
        for n in start_functions:
            ip = function_pos['ip']
            jobs.append(gevent.spawn(self.trigger_function, workflow_name, transaction_id, n, ip, expired_keys))
        gevent.joinall(jobs)
        end = time.time()
        return end - start

    def wait_Tx_repair_finish(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        while not self.running_txs[tx_id]['finished']:
            with condition:
                condition.wait()

    def notify_Tx(self, tx_id, success):
        condition = self.running_txs[tx_id]['cond']
        self.running_txs[tx_id]['finished'] = True
        self.running_txs[tx_id]['repaired'] = success
        with condition:
            condition.notify_all()
        print(f"notified {tx_id}, repaired: {success}")

