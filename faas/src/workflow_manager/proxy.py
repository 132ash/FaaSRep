import sys
import logging
# 配置日志记录
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    # 设置日志级别为 INFO
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S',  # 设置日期格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ],
    force=True 
)


from gevent import monkey
monkey.patch_all()
import os
import gevent
import time
import requests
import json
from typing import Dict
from datetime import datetime
sys.path.append('../../config')
import config
import workersp_repo
from workersp import WorkerSPManager, TransactionState
import docker
from flask import Flask, request
app = Flask(__name__)
docker_client = docker.from_env()
container_names = []
repo = workersp_repo.Repository()
sys.path.append('../../config')
import config

default_FaaSTCC_snapshot_interval = [datetime(2000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f'), datetime(2999, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')]



REPAIRED = 1
ABORTED = 2

class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       print("Clearing previous containers.")
       os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
       repo.clear_mem()
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.node_list = repo.get_all_addrs('common')
       self.managers = {name: WorkerSPManager(self.host_addr, name, addr,  repo, self.node_list) for name, addr in info_addrs.items()}

    def get_state(self, workflow_name, retry_after_abort, transaction_id, write_set, snapshot_interval) -> TransactionState:
        return self.managers[workflow_name].get_state(retry_after_abort, transaction_id, write_set, snapshot_interval)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)
    
    def clear_mem(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_mem(transaction_id)

    def FaaSTCC_abort(self, workflow_name, transaction_id):
        self.managers[workflow_name].abort_tx(transaction_id)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)

    def stop_transaction(self, workflow_name, transaction_id):
        self.managers[workflow_name].stop_transaction(transaction_id)



dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)
if config.FILLUP_CACHE:
    repo.fillup_cache()

@app.route('/abort', methods = ['POST'])
def abort():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
    payload = {
        'transaction_id_list': [[transaction_id]],
        'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
        'abort': True
    }
    requests.post(notify_url, json=payload)
    return json.dumps({'status': 'ok'})

# a new request from outside
# the previous function was done
@app.route('/request', methods = ['POST'])
def req():
    start = time.time()
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    function_name = data['function_name']
    no_parent_execution = data['no_parent_execution']
    retry_after_abort = data.get('retry', False)
    write_set = data.get('write_set', {})
    snapshot_interval = data.get('snapshot_interval', default_FaaSTCC_snapshot_interval)
    state = dispatcher.get_state(workflow_name, retry_after_abort, transaction_id, write_set, snapshot_interval)
    if state is None:
        dispatcher.FaaSTCC_abort(workflow_name, transaction_id)
        return
    logging.info(f"request [{transaction_id}],  workflow_name: {workflow_name}, function_name: {function_name},snapshot interval:{state.snapshot_interval}")
    # get the corresponding workflow state and trigger the function
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

# commit data on this node, and return the containers to the pool
@app.route('/commit', methods = ['POST'])
def commit():
    data = request.get_json(force=True, silent=True)
    version = data['version']
    tx_list = data['txs']
    commit_key_list = data.get('keys', [])
    # release the containers reserved into container pool.
    logging.info(f"Worker commit. all transactions:{tx_list} commit_key_list: {commit_key_list}, version {version}")
    repo.commit_tx_writes(commit_key_list, tx_list, version)
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
# python3 proxy.py  192.168.162.131 7000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   