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

validate_interval = 0.005 # 200 qps at most
default_FaaSTCC_snapshot_interval = [datetime(2000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f'), datetime(2999, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')]

REPAIRED = 1
ABORTED = 2

class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       print("Clearing previous containers.")
       os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.node_list = repo.get_all_addrs('common')
       self.managers = {name: WorkerSPManager(self.host_addr, name, addr,  repo, self.node_list) for name, addr in info_addrs.items()}

    def get_state(self, create_timestamp, retry_after_abort, workflow_name, transaction_id, write_set, lock_set) -> TransactionState:
        return self.managers[workflow_name].get_state(create_timestamp, retry_after_abort, transaction_id, write_set,lock_set)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)

    def stop_transaction(self, workflow_name, transaction_id):
        self.managers[workflow_name].stop_transaction(transaction_id)


dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)

# a new request from outside
# the previous function was done
@app.route('/request', methods = ['POST'])
def req():
    start = time.time()
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    function_name = data['function_name']
    create_timestamp = data['create_timestamp']
    no_parent_execution = data['no_parent_execution']
    retry_after_abort = data.get('retry', False)
    write_set = data.get('write_set', {})
    lock_set = data.get('lock_set', {})
    state = dispatcher.get_state(create_timestamp, retry_after_abort, workflow_name, transaction_id,  write_set, lock_set)
    ## logging.info(f"request [{transaction_id}], workflow_name: {workflow_name}, function_name: {function_name}, lock_set:{lock_set} get state latency:{time.time()-start}")
    # get the corresponding workflow state and trigger the function
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    dispatcher.del_state(workflow_name, transaction_id) # and remove state for every node
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

    
# python3 proxy.py  10.2.30.52 7500
# python3 proxy.py  10.2.27.24 7500
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   