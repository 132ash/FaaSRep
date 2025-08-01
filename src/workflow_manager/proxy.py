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
import json
from typing import Dict
from datetime import datetime
import config
import workersp_repo
from workersp import WorkerSPManager, TransactionState
import docker
from workersp import ReservePool
from flask import Flask, request
app = Flask(__name__)
docker_client = docker.from_env()
repo = workersp_repo.Repository()

sys.path.append('../../config')
import config

validate_interval = 0.005 # 200 qps at most
default_FaaSTCC_snapshot_interval = [datetime(2000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f'), datetime(2999, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')]


FAST_PATH = config.FAST_PATH
PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR

REPAIRED = 1
ABORTED = 2

class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       print("Clearing previous containers.")
       os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       repo.shadowtable_init(sys.argv[1])
       repo.clear_mem()
       self.node_list = repo.get_all_addrs('common')
       # logging.info(f"Node list: {self.node_list}")
       self.reserve_pools =  {name: ReservePool() for name in info_addrs}
       self.managers = {name: WorkerSPManager(self.host_addr, name, addr, self.reserve_pools[name], repo, self.node_list) for name, addr in info_addrs.items()}
       
    def get_state(self, retry_after_abort, workflow_name, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_mode, repair_states) -> TransactionState:
        return self.managers[workflow_name].get_state(retry_after_abort, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_mode, repair_states)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)

    def trigger_crosstx_function(self, workflow_name, function_name, transaction_id):
        self.managers[workflow_name].crosstx_trigger_function(transaction_id, function_name)

    def trigger_repair(self, batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port, repair_mode):
        self.managers[workflow_name].trigger_repair(batch_id, transaction_id, function_name, no_parent_execution, port, repair_mode)

    def clear_mem(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_mem(transaction_id)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)
    



dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)
if config.FILLUP_CACHE:
    repo.fillup_cache()


# trigger a reserved container. If fail, run another container.
@app.route('/repair', methods = ['POST'])
def repair():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    function_name = data['function_name']
    repair_mode = data['repair_mode']
    no_parent_execution = data['no_parent_execution']
    port = data['port']
    # logging.info(f"FASTPATH repair. batch_id: {batch_id}, transaction_id: {transaction_id}, workflow_name: {workflow_name}, function_name: {function_name}, no_parent_execution: {no_parent_execution}, port: {port}")
    dispatcher.trigger_repair(batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port, repair_mode)
    return json.dumps({'status': 'ok'})

@app.route('/crosstx_req', methods = ['POST'])
def crosstx_req():
    data = request.get_json(force=True, silent=True)
    function_name = data['function_name']
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    dispatcher.trigger_crosstx_function(workflow_name, function_name, transaction_id)
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
    # collected repair metadata, transported between functions. 
    container_port = data.get('container_port', {})
    read_set = data.get('read_set', {})
    write_set = data.get('write_set', {})
    RYW_subjection = data.get('RYW_subjection', {})
    # data for repair
    batch_id = data.get('batch_id', "")
    repair = data.get('repair', False)
    repair_mode = data.get('repair_mode', "")
    repair_states = data.get('repair_states', {})
    state = dispatcher.get_state(retry_after_abort, workflow_name, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair,repair_mode, repair_states)
    # get the corresponding workflow state and trigger the function
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    abort_clear = data.get('abort', False)
    dispatcher.del_state(workflow_name, transaction_id) # and remove state for every node
    if abort_clear:
        if FAST_PATH:
            dispatcher.reserve_pools[workflow_name].release([transaction_id])
    else:
        dispatcher.clear_mem(workflow_name, transaction_id) # must clear memory after each run 
    return json.dumps({'status': 'ok'})


@app.route('/prepare', methods = ['POST'])
def prepare():
    data = request.get_json(force=True, silent=True)
    repair_metadata = data['repair_metadata'] # {txid: {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx}]}
    
    # update cache on this node.
    repo.update_cache(data['expired_keys'])
    if repair_metadata:
        repo.fillup_repair_matadata(repair_metadata)

    return json.dumps({'status': 'ok'})

# commit data on this node, and return the containers to the pool
@app.route('/commit', methods = ['POST'])
def commit():
    data = request.get_json(force=True, silent=True)
    commit_list = data['commit_list']
    if FAST_PATH:
        workflow_name = data['workflow_name']
        fin_tx_list = commit_list['txs']
        dispatcher.reserve_pools[workflow_name].release(fin_tx_list)
    repo.commit_tx_writes(commit_list['keys'])
    return json.dumps({'status': 'ok'})



@app.route('/clear_container', methods = ['GET'])
def clear_container():
    print('clearing containers')
    os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
    return json.dumps({'status': 'ok'})

GET_NODE_INFO_INTERVAL = 0.1





# python3 proxy.py  10.2.30.52 7500
# python3 proxy.py  10.2.27.24 7500
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   