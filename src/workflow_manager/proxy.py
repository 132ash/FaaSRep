import sys
import logging
import os
# 配置日志记录 - 输出到文件并每次运行时刷新
log_file = '../../logging/proxy.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

def setup_logger():
    logger = logging.getLogger('proxy')
    logger.setLevel(logging.INFO)
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# 全局logger实例
logger = setup_logger()

def #log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()

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

    def get_state(self, create_timestamp, workflow_name, transaction_id, write_set, term) -> TransactionState:
        return self.managers[workflow_name].get_state(create_timestamp, transaction_id, write_set, term)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id, fin):
        self.managers[workflow_name].del_state(transaction_id, fin)

dispatcher = Dispatcher(info_addrs=config.WORKFLOW_YAML_ADDR)

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
    write_set = data.get('write_set', {})
    term = data['term']
    state = dispatcher.get_state(create_timestamp, workflow_name, transaction_id,  write_set, term)
    ## logging.info(f"request [{transaction_id}], workflow_name: {workflow_name}, function_name: {function_name}, lock_set:{lock_set} get state latency:{time.time()-start}")
    # get the corresponding workflow state and trigger the function
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    fin = data.get('fin', False)
    dispatcher.del_state(workflow_name, transaction_id, fin) # and remove state for every node
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

    
# python3 proxy.py  10.2.30.50 7500
# python3 proxy.py  10.2.30.62 7500
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   