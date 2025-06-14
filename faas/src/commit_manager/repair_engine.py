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
PESSIMISTIC_REPAIR_ENABLED = config.PESSIMISTIC_REPAIR and config.REPAIR

repo = validator_repo.Repository()

class RepairEngine:

    def __init__(self, repair_info:RepairInfo, workflow_name, tx_sink_addr):
        self.repair_info = repair_info
        self.tx_sink_addr = tx_sink_addr
        self.workflow_name = workflow_name
        self.start_functions = repo.get_start_functions(self.workflow_name + '_workflow_metadata')

    def repair_batch(self,batch_id, transaction_list, function_pos_per_tx, expired_keys, worker_ip_set, pessi_sink_info):
        # allocate works
        start = time.time()
        repair_metadata_jobs = []
        if FAST_PATH_ENABLED:
            for ip in worker_ip_set:
                repair_metadata_local = self.repair_info.get_repair_metadata(batch_id, ip)
                repair_metadata_jobs.append(gevent.spawn(self.prepare_repairing_on_worker, batch_id, ip, repair_metadata_local, expired_keys.get(ip, [])))
            gevent.joinall(repair_metadata_jobs) 
        
        # metadata filled. Trigger start functions to repair workflow.
        trigger_jobs = []
        repair_metadata_no_fast = {}
        for tx_id in transaction_list:
            if not FAST_PATH_ENABLED:
                repair_metadata_no_fast = self.repair_info.get_repair_metadata(batch_id, '', tx_id)
            if PESSIMISTIC_REPAIR_ENABLED:
                pessi_sink_info['current_info'].setdefault(tx_id, {'cnt':0, 'successors':[], 'start_func':[], 'repair_states':{}})
                pessi_sink_info['current_info'][tx_id]['repair_states'] = repair_metadata_no_fast
            # trigger start functions
            for n in self.start_functions:
                ip = function_pos_per_tx[tx_id][n]['ip']
                port = function_pos_per_tx[tx_id][n]['port']
                if PESSIMISTIC_REPAIR_ENABLED:
                    pessi_sink_info['current_info'][tx_id]['start_func'].append({'name':n, 'ip':ip, 'port':port})
                else:
                    trigger_jobs.append(gevent.spawn(self.trigger_function, FAST_PATH_ENABLED, self.workflow_name, tx_id, n, ip, port,batch_id, repair_metadata_no_fast))
        if PESSIMISTIC_REPAIR_ENABLED:
            trigger_jobs.append(gevent.spawn(self.trigger_repair_on_sink, batch_id, pessi_sink_info))
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

    def trigger_repair_on_sink(self,batch_id, pessi_sink_info):
        ip = self.tx_sink_addr
        if not ip.endswith(":7000"):
            url = f'http://{ip}:7000/repair_pessi'
        else:
            url = f'http://{ip}/repair_pessi'
        data = {
            'batch_id': batch_id,
            'workflow_name': self.workflow_name,
            'prev_batch_info': pessi_sink_info['prev_batch_info'],
            'current_batch_info': pessi_sink_info['current_batch_info']  
        }
        requests.post(url, json=data)


    # repair_metadata: {txid:{func:{ RYW:xx, dirty:xx, downstream:xx, upstream:xx}}}
    # send metadata to the proxy on worker node.
    # all functions' ip and port need to be sent(?)
    def prepare_repairing_on_worker(self, batch_id, worker_ip, repair_metadata, expired_keys):
        if not worker_ip.endswith(":7000"):
            url = 'http://{}:7000/prepare'.format(worker_ip)
        else:
            url = 'http://{}/prepare'.format(worker_ip)
        data = {
            'batch_id': batch_id,
            'repair_metadata': repair_metadata,
            'expired_keys': expired_keys
        }
        print(f"fillup metadata on worker {worker_ip}, batch_id: {batch_id}, metadata: {repair_metadata}")
        requests.post(url, json=data)

