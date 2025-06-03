from gevent import monkey
monkey.patch_all()
import requests
import gevent
import sys
from gevent import event
import time
import logging
import validator_repo

sys.path.append('../../config')
import config
from repair_info import RepairInfo

FAST_PATH_ENABLED = config.FAST_PATH and config.REPAIR

repo = validator_repo.Repository()

class RepairEngine:

    def __init__(self, repair_info:RepairInfo, workflow_name):
        self.repair_info = repair_info
        self.workflow_name = workflow_name

    def repair_batch(self,batch_id, transaction_list, function_pos_per_tx, expired_keys, worker_ip_set):
        # allocate works
        start = time.time()
        repair_metadata_jobs = []
        for ip in worker_ip_set:
            repair_metadata_local = {}
            if FAST_PATH_ENABLED:
                repair_metadata_local = self.repair_info.get_repair_metadata(batch_id, ip)
            repair_metadata_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, function_pos_per_tx, repair_metadata_local, expired_keys.get(ip, [])))
        gevent.joinall(repair_metadata_jobs) 
        
        # metadata filled. Trigger start functions to repair workflow.
        trigger_jobs = []
        for tx_id in transaction_list:
            start_functions = repo.get_start_functions(self.workflow_name + '_workflow_metadata')
            for n in start_functions:
                ip = function_pos_per_tx[tx_id][n]['ip']
                port = function_pos_per_tx[tx_id][n]['port']
                repair_metadata_per_tx = self.repair_info.get_repair_metadata(batch_id, "", tx_id) if not FAST_PATH_ENABLED else {}
                trigger_jobs.append(gevent.spawn(self.trigger_function, FAST_PATH_ENABLED, self.workflow_name, tx_id, n, ip, port,batch_id,repair_metadata_per_tx))
        gevent.joinall(trigger_jobs)   
        end = time.time()
        return end - start

    def trigger_function(self, FAST_PATH_ENABLED, workflow_name, transaction_id, function_name, ip, port, batch_id, repair_metadata_per_tx):
        route = "repair" if FAST_PATH_ENABLED else "request"
        if not ip.endswith(":7000"):
            url = f'http://{ip}:7000/{route}'
        else:
            url = f'http://{ip}/{route}'
        # print(f"-----repair function: {function_name}, ip: {ip}, port: {port}, batch_id: {batch_id}, repair_states:{repair_metadata_per_tx}-----")
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'port': port,
            'repair': True,
            'repair_states':repair_metadata_per_tx
            
        }
        print(f"triggering {function_name}, sending req to {url}, batch_id: {batch_id}")
        requests.post(url, json=data)


    # repair_metadata: {txid:{func:{ RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}
    # send metadata to the proxy on worker node.
    # all functions' ip and port need to be sent(?)
    def prepare_repairing_on_worker(self, batch_id, worker_ip, function_pos_per_tx, repair_metadata, expired_keys):
        if not worker_ip.endswith(":7000"):
            url = 'http://{}:7000/prepare'.format(worker_ip)
        else:
            url = 'http://{}/prepare'.format(worker_ip)
        data = {
            'batch_id': batch_id,
            'repair_metadata': repair_metadata,
            'function_pos': function_pos_per_tx,
            'expired_keys': expired_keys
        }
        print(f"fillup metadata on worker {worker_ip}, batch_id: {batch_id}, metadata: {repair_metadata}")
        requests.post(url, json=data)

