import sys
import logging
import os
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / 'config'))
from experiment_logging import make_experiment_logger, configure_root_experiment_logging

def setup_logger():
    configure_root_experiment_logging('workersp_runtime')
    return make_experiment_logger('workflow_manager_proxy', 'workflow_manager_proxy')

# 全局logger实例
logger = setup_logger()

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()

from gevent import monkey
monkey.patch_all()
import os
import time
import json
from typing import Dict
from datetime import datetime
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
        log_message("Clearing previous containers.")
        os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
        time.sleep(3)
    # Ensure no related containers exist before continuing
        try:
            containers = docker_client.containers.list(all=True, filters={'label': 'workflow'})
            if containers:
                for container in containers:
                    try:
                        container.remove(force=True)
                    except Exception as e:
                        log_message(f"Failed to remove container {container.id}: {e}")
                time.sleep(1)  # Wait a moment for cleanup to complete
            #log_message("All workflow containers have been cleared.")
        except Exception as e:
            log_message(f"Error during container cleanup: {e}")
        self.host_addr = sys.argv[1] + ':' + sys.argv[2]
        repo.shadowtable_init(sys.argv[1])
        repo.clear_mem()
        self.node_list = repo.get_all_addrs('common')
        ##log_message(f"Node list: {self.node_list}")
        self.all_workflows = info_addrs.keys()
        self.reserve_pools =  {name: ReservePool() for name in info_addrs}
        self.managers = {name: WorkerSPManager(self.host_addr, name, addr, self.reserve_pools[name], repo, self.node_list) for name, addr in info_addrs.items()}
        #log_message(f"Dispatcher initialized with workflows: {list(self.managers.keys())}")


    def get_state(self, retry_after_abort, workflow_name, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_mode, repair_states, transaction_metadata=None, repair_epoch=0, attempt_id='') -> TransactionState:
        return self.managers[workflow_name].get_state(retry_after_abort, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_mode, repair_states, transaction_metadata, repair_epoch, attempt_id)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)

    def trigger_crosstx_function(self, workflow_name, function_name, transaction_id):
        self.managers[workflow_name].crosstx_trigger_function(transaction_id, function_name)

    def trigger_repair(self, batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port, repair_mode, repair_epoch, attempt_id):
        self.managers[workflow_name].trigger_repair(batch_id, transaction_id, function_name, no_parent_execution, port, repair_mode, repair_epoch, attempt_id)

    def clear_mem(self, workflow_name, transaction_id):
        return self.managers[workflow_name].clear_mem(transaction_id)

    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)

    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)

    def clear_containers(self):
        for workflow_name in self.all_workflows:
            self.managers[workflow_name].clear_containers()




dispatcher = Dispatcher(info_addrs=config.WORKFLOW_YAML_ADDR)
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
    repair_epoch = data.get('repair_epoch', 1)
    attempt_id = data.get('attempt_id', '')
    no_parent_execution = data['no_parent_execution']
    port = data['port']
    ##log_message(f"FASTPATH repair. batch_id: {batch_id}, transaction_id: {transaction_id}, workflow_name: {workflow_name}, function_name: {function_name}, no_parent_execution: {no_parent_execution}, port: {port}")
    dispatcher.trigger_repair(batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port, repair_mode, repair_epoch, attempt_id)
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
    transaction_metadata = data.get('transaction_metadata', {})
    repair_epoch = data.get('repair_epoch', 1 if repair else 0)
    attempt_id = data.get('attempt_id', '')
    # if repair:
        ##log_message(f"Repair request received: transaction_id: {transaction_id}, workflow_name: {workflow_name}, function_name: {function_name}, no_parent_execution: {no_parent_execution}, retry_after_abort: {retry_after_abort}, container_port: {container_port}, read_set: {read_set}, write_set: {write_set}, RYW_subjection: {RYW_subjection}, batch_id: {batch_id}, repair: {repair}, repair_mode: {repair_mode}, repair_states: {repair_states}")
    state = dispatcher.get_state(retry_after_abort, workflow_name, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair,repair_mode, repair_states, transaction_metadata, repair_epoch, attempt_id)
    if repair and (state.repair_epoch, state.repair_mode, state.attempt_id) != (repair_epoch, repair_mode, attempt_id):
        log_message(json.dumps({'event': 'STALE_TRIGGER_REJECTED', 'workflow': workflow_name,
                                'batch_id': batch_id, 'tx_id': transaction_id,
                                'function': function_name, 'repair_mode': repair_mode,
                                'repair_epoch': repair_epoch, 'attempt_id': attempt_id,
                                'timestamp': time.time()}))
        return json.dumps({'status': 'stale'})
    # get the corresponding workflow state and trigger the function
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    abort_clear = data.get('abort', False)
    # Containers retain optimistic contexts because a predecessor abort may
    # promote the transaction to pessimistic repair. Clear them only after the
    # gateway has observed a terminal outcome.
    # if abort_clear:
    #     if FAST_PATH:
    #         ##log_message(f"transaction {transaction_id} abort, return its containers to pool.")
    #         dispatcher.reserve_pools[workflow_name].release([transaction_id])
    # else:
    participated = dispatcher.clear_mem(workflow_name, transaction_id)
    if participated:
        dispatcher.del_state(workflow_name, transaction_id)
    return json.dumps({
        'status': 'ok',
        'cleared': participated,
    })

@app.route('/prepare', methods = ['POST'])
def prepare():
    data = request.get_json(force=True, silent=True)
    repair_metadata = data['repair_metadata'] # {txid: {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx}]}
    # update cache on this node.
    repo.update_cache(data['expired_keys'])

    if repair_metadata:
        repo.fillup_repair_matadata(repair_metadata, data['mode'])

    return json.dumps({'status': 'ok'})

# commit data on this node, and return the containers to the pool
@app.route('/commit', methods = ['POST'])
def commit():
    data = request.get_json(force=True, silent=True)
    commit_list = data['commit_list']
    aborted_txs = commit_list['aborted_txs']
    # if FAST_PATH:
    #     workflow_name = data['workflow_name']
    #     fin_tx_list = commit_list['txs']
    #     #log_message(f"transactions {fin_tx_list} committing, release containers.")
    #     dispatcher.reserve_pools[workflow_name].release(fin_tx_list)
    repo.commit_tx_writes(commit_list['keys'])
    #repo.clear_aborted_txs(aborted_txs)
    #log_message(f"transactions {fin_tx_list} committed, aborted_txs:{aborted_txs}")
    return json.dumps({'status': 'ok'})

@app.route('/release', methods = ['POST'])
def release():
    data = request.get_json(force=True, silent=True)
    tx_lists = data['tx_lists']
    workflow_name = data['workflow_name']
    for fin_tx_list in tx_lists:
        ##log_message(f"transactions {fin_tx_list} commited, release containers.")
        dispatcher.reserve_pools[workflow_name].release(fin_tx_list)
    return json.dumps({'status': 'ok'})


@app.route('/clear_container', methods = ['POST'])
def clear_container():
    dispatcher.clear_containers() # and remove state for every node
    return json.dumps({'status': 'ok'})

GET_NODE_INFO_INTERVAL = 0.1





# python3 proxy.py  10.2.30.50 7500
# python3 proxy.py  10.2.27.23 7500
# python3 proxy.py  10.2.30.62 7500
# python3 proxy.py  10.2.29.142  7500

from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app,
                        log=None, error_log=None)
    server.serve_forever()
