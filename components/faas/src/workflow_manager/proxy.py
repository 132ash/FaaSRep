from gevent import monkey
monkey.patch_all()
import os
import gevent
import requests
import json
from typing import Dict
import sys
import gevent.lock
sys.path.append('../../config')
import config
import workersp_repo
from workersp import WorkerSPManager
import docker
from flask import Flask, request
app = Flask(__name__)
docker_client = docker.from_env()
container_names = []
repo = workersp_repo.Repository()

validate_interval = 0.005 # 200 qps at most

class ValidationQueue:
    def __init__(self, batch_size):
        self.queue = []
        self.queue_lock = gevent.lock.BoundedSemaphore()
        self.batch_size = batch_size
        self.repairing_batch_table: Dict[str, Dict] = {}

    def append(self, transaction_id: str, workflow_name:str, read_set: Dict[str, Dict], write_set: Dict[str, int], function_pos: Dict[str, str]):
        self.queue_lock.acquire()
        self.queue.append({'transaction_id': transaction_id, "workflow_name":workflow_name, 
                           'read_set': read_set, 'write_set': write_set, 'function_pos': function_pos})
        self.queue_lock.release()

    # transform the batch from a list of txs to a dict, for the convenience of validation.
    # readset and writeset are lists for locking in sequence, so they are not transformed.
    def transform_batch(self, batch):
        transformed_batch = {
            "batch_id": batch[0]["transaction_id"],
            "workflow_name": {},
            "read_set": {},
            "write_set": {},
            "function_pos": {},
            "transaction_list":[]
        }
        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["workflow_name"][tx_id] = tx["workflow_name"]
            transformed_batch["read_set"][tx_id]=tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["function_pos"][tx_id] = tx["function_pos"]
            transformed_batch["transaction_list"].append(tx_id)
        return transformed_batch

    def send_validate_request(self):
        self.queue_lock.acquire()
        if len(self.queue) == 0:
            self.queue_lock.release()
            return
        idx = min(self.batch_size, len(self.queue))
        batch = self.transform_batch(self.queue[:idx])
        self.queue = self.queue[idx:]
        self.queue_lock.release()
        batch_id = batch[0]["transaction_id"]
        self.repairing_batch_table[batch_id] = {"batch_size": idx, "finished": 0, "lock": gevent.lock.BoundedSemaphore()}
        remote_url = 'http://{}/validate'.format(config.VALIDATOR_ADDR)
        data = {
            "batch": batch,
            "batch_id": batch_id
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

class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       repo.clear_mem()
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.cache_updated_table = {}
       self.validation_queue = ValidationQueue(config.BATCH_SIZE)
       self.managers = {name: WorkerSPManager(self.host_addr, name, addr, self.validation_queue) for name, addr in info_addrs.items()}
       gevent.spawn_later(validate_interval, self._validate_loop)

    def get_state(self, workflow_name: str, transaction_id: str, function_pos={}, read_set={}, write_set={}, repair=False, repair_states={}) -> WorkerSPManager:
        return self.managers[workflow_name].get_state(transaction_id, function_pos, read_set, write_set , repair,repair_states)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)
    
    def clear_mem(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_mem(transaction_id)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)
    
    def _validate_loop(self):
        gevent.spawn_later(validate_interval, self._validate_loop)
        gevent.spawn(self.validation_queue.send_validate_request)

    def update_cache(self, batch_id, expired_keys):
        # not updated yet, and have expired keys
        if self.cache_updated_table.get(batch_id, False):
            repo.update_cache(expired_keys.get(self.host_addr, []))
            self.cache_updated_table[batch_id] = True
            expired_keys.pop(self.host_addr, None)



dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)
if config.FILLUP_CACHE:
    repo.fillup_cache()

# a new request from outside
# the previous function was done
@app.route('/request', methods = ['POST'])
def req():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    function_name = data['function_name']
    no_parent_execution = data['no_parent_execution']
    function_pos = data.get('function_pos', {})
    # sent from the previous function
    read_set = data.get('read_set', {})
    write_set = data.get('write_set', {})
    # data for repair
    repair = data.get('repair', False)
    repair_states = {}
    repair_states['expired_keys'] = data.get('expired_keys', {})
    repair_states['dirty_set'] = data.get('dirty_set', {})
    repair_states['downstream_func_table'] = data.get('downstream_func_table', {})
    repair_states['upstream_func_table'] = data.get('upstream_func_table', {})
    repair_states['batch_id'] = data.get('batch_id', 0)
    # update expired keys.
    if repair:
        dispatcher.update_cache(repair_states['batch_id'], repair_states['expired_keys'])
        # a cross tx trigger, this function must re-run.
        if data.get('crosstx', False):
            repair_states['function_name'] = True
    

    print(f"--------request {workflow_name} {transaction_id} {function_name}, repair:{repair}")
    # get the corresponding workflow state and trigger the function
    state = dispatcher.get_state(workflow_name, transaction_id, function_pos, read_set, write_set, repair, repair_states)
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    dispatcher.clear_mem(workflow_name, transaction_id) # must clear memory after each run 
    dispatcher.del_state(workflow_name, transaction_id) # and remove state for every node
    return json.dumps({'status': 'ok'})

@app.route('/commit', methods = ['POST'])
def commit():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    transaction_id_list = data['transaction_id_list']
    version = data['version']
    print(f"commit txid: {transaction_id_list}, version {version}")
    repo.commit_tx_writes(transaction_id_list, version)
    dispatcher.cache_updated_table.pop(batch_id, None)
    return json.dumps({'status': 'ok'})

@app.route('/info', methods = ['GET'])
def info():
    return json.dumps(container_names)

@app.route('/clear_container', methods = ['GET'])
def clear_container():
    print('clearing containers')
    os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
    return json.dumps({'status': 'ok'})

GET_NODE_INFO_INTERVAL = 0.1

def get_container_names():
    gevent.spawn_later(get_container_names)
    global container_names
    container_names = [container.attrs['Name'] for container in docker_client.containers.list()]

    
# python proxy.py  192.168.162.130 7000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   