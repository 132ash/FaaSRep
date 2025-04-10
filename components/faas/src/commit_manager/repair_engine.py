from gevent import monkey
monkey.patch_all()
import requests
import gevent
import sys
from gevent import event
import time
import validator_repo

sys.path.append('../../config')
from repair_info import RepairInfo

repo = validator_repo.Repository()

class RepairEngine:

    def __init__(self, repair_info:RepairInfo):
        self.repairing_batches = {}
        self.repair_info = repair_info
        
    def trigger_repair(self, batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, worker_ip_set):
        self.repairing_batches[batch_id] = {'cond': event.Event(), 'finished': False, "repaired":False}
        self.repairing_batches[batch_id]['cond'].clear()
        self.repair_batch(batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, worker_ip_set)
        self.repairing_batches[batch_id]['cond'].wait()
        if self.repairing_batches[batch_id]['repaired']:
            return True
        else:
            return False

    def trigger_function(self, workflow_name, transaction_id, function_name, ip, port, batch_id):
        if not ip.endswith(":7000"):
            url = 'http://{}:7000/repair'.format(ip)
        else:
            url = 'http://{}/repair'.format(ip)
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': True,
            'port': port
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


    def repair_batch(self,batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, worker_ip_set):
        # allocate works
        start = time.time()
        repair_metadata_jobs = []
        for ip in worker_ip_set:
            repair_metadata_local = self.repair_info.get_repair_metadata_for_ip(batch_id, ip)
            repair_metadata_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, function_pos_per_tx, repair_metadata_local, expired_keys.get(ip, [])))
        gevent.joinall(repair_metadata_jobs) 
        
        # metadata filled. Trigger start functions to repair workflow.
        trigger_jobs = []
        for tx_id in transaction_list:
            workflow_name = workflow_name_per_tx[tx_id]
            start_functions = repo.get_start_functions(workflow_name + '_workflow_metadata')
            for n in start_functions:
                ip = function_pos_per_tx[tx_id][n]['ip']
                port = function_pos_per_tx[tx_id][n]['port']
                print(f"start functions: {start_functions}, tx_id: {tx_id}, workflow_name: {workflow_name}, function: {n}, ip: {ip}")
                trigger_jobs.append(gevent.spawn(self.trigger_function, workflow_name, tx_id, n, ip, port,batch_id))
        gevent.joinall(trigger_jobs)   
        end = time.time()
        return end - start

    def notify_batch(self, batch_id, success):
        self.repairing_batches[batch_id]['repaired'] = success
        self.repairing_batches[batch_id]['cond'].set()
        print(f"notified {batch_id}, repaired: {success}")

