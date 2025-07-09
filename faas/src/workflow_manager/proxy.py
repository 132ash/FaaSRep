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
from validate_struct import TransactionSink, ReservePool
from Concord_cache_agent import ConcordCacheAgent

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
       repo.clear_mem()
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.node_list = repo.get_all_addrs('common')
       self.reserve_pools =  {name: ReservePool() for name in info_addrs}
       self.sinks = {name: TransactionSink(name, config.BATCH_SIZE, self.host_addr, repo) for name in info_addrs}  
       self.managers = {name: WorkerSPManager(self.host_addr, name, addr, self.sinks[name], self.reserve_pools[name], repo, self.node_list) for name, addr in info_addrs.items()}
       self.concord_cache_agent = {name:ConcordCacheAgent(name, repo, self.host_addr)  for name in info_addrs}
       gevent.spawn_later(validate_interval, self._validate_loop)

    def fin_repair_or_abort_within_batch(self, workflow_name, batch_id, transaction_id, state):
        self.sinks[workflow_name].fin_repair_or_abort(batch_id, transaction_id, state)
        self.reserve_pools[workflow_name].release(transaction_id)

    def register_pessimistic_info(self, workflow_name, batch_id, batch_sub, tx_sub):
        return self.sinks[workflow_name].register_pessimistic_info(batch_id, batch_sub, tx_sub)

    def get_state(self, workflow_name, transaction_id, read_set, write_set, batch_id, RYW_subjection,repair, repair_states, lock_set={}, snapshot_interval=[]) -> TransactionState:
        return self.managers[workflow_name].get_state(transaction_id, read_set, write_set, batch_id, RYW_subjection, repair, repair_states,lock_set,snapshot_interval)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)

    def trigger_crosstx_function(self, workflow_name, function_name, transaction_id):
        self.managers[workflow_name].crosstx_trigger_function(transaction_id, function_name)
   
    def trigger_repair(self, batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port):
        self.managers[workflow_name].trigger_repair(batch_id, transaction_id, function_name, no_parent_execution, port)
    
    def clear_mem(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_mem(transaction_id)

    def FaaSTCC_abort(self, workflow_name, transaction_id):
        self.managers[workflow_name].abort_tx(transaction_id)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id):
        self.managers[workflow_name].del_state(transaction_id)
    
    def _validate_loop(self):
        gevent.spawn_later(validate_interval, self._validate_loop)
        for sink in self.sinks.values():
            gevent.spawn(sink.send_validate_request)



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
    no_parent_execution = data['no_parent_execution']
    port = data['port']
    logging.info(f"FASTPATH repair. batch_id: {batch_id}, transaction_id: {transaction_id}, workflow_name: {workflow_name}, function_name: {function_name}, no_parent_execution: {no_parent_execution}, port: {port}")
    dispatcher.trigger_repair(batch_id, transaction_id, workflow_name, function_name, no_parent_execution, port)
    return json.dumps({'status': 'ok'})

@app.route('/fin_repair', methods = ['POST'])
def fin_repair():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    dispatcher.fin_repair_or_abort_within_batch(workflow_name, batch_id, transaction_id, REPAIRED)
    return json.dumps({'status': 'ok'})

@app.route('/abort', methods = ['POST'])
def abort():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    if config.REMOTE_LOCK:
        lock_set = data.get('lock_set', {})
        repo.release_lock(transaction_id, lock_set)
    elif config.PESSIMISTIC_REPAIR and data.get('repair', False):
        dispatcher.fin_repair_or_abort_within_batch(workflow_name, data['batch_id'], transaction_id, ABORTED)
    notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
    payload = {
        'transaction_id_list': [[transaction_id]],
        'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
        'abort': True
    }
    requests.post(notify_url, json=payload)
    return json.dumps({'status': 'ok'})

@app.route('/crosstx_req', methods = ['GET'])
def crosstx_req():
    data = request.get_json(force=True, silent=True)
    function_name = data['function_name']
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    dispatcher.trigger_crosstx_function(workflow_name, function_name, transaction_id)


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
    # sent from the previous function
    container_port = data.get('container_port', {})
    read_set = data.get('read_set', {})
    write_set = data.get('write_set', {})
    RYW_subjection = data.get('RYW_subjection', {})
    # data for repair
    batch_id = data.get('batch_id', "")
    repair = False
    repair_states = {}
    # data for remote lock
    lock_set = data.get('lock_set', {})
    # data for FaaSTCC
    snapshot_interval = data.get('snapshot_interval', default_FaaSTCC_snapshot_interval)
    if config.REPAIR and not config.FAST_PATH:
        repair = data.get('repair', False)
        repair_states = data.get('repair_states', {})
    state = dispatcher.get_state(workflow_name, transaction_id, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_states, lock_set, snapshot_interval)
    if config.FAASTCC and state is None:
        dispatcher.FaaSTCC_abort(workflow_name, transaction_id)
        return
    logging.info(f"request [{transaction_id}], REPAIR:{repair} workflow_name: {workflow_name}, function_name: {function_name}, get state latency:{time.time()-start}")
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

@app.route('/repair_pessi', methods = ['POST'])
def repair_pessimistic():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    batch_id = data['batch_id']
    batch_sub =  data['batch_sub']
    tx_sub =  data['tx_sub']  
    return json.dumps(dispatcher.register_pessimistic_info(workflow_name, batch_id, batch_sub, tx_sub))


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

@app.route('/concord_data', methods = ['GET'])
def concord_data():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    key = data['key']
    workflow = data['workflow']
    trigger_tx = data['trigger_tx']
    value = ''
    if mode == 'invalidated':
        dispatcher.concord_cache_agent[workflow].invalidate_by_home(key, trigger_tx)
    else:
        value = dispatcher.concord_cache_agent[workflow].data_access(trigger_tx, key, mode)
    return json.dumps({'value': value})

@app.route('/concord_lock', methods = ['GET'])
def concord_lock():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    lock_keys = data['lock_keys']
    lock = data['lock']  # default to True, meaning acquire locks
    dispatcher.concord_cache_agent[data['workflow']].lock_or_unlock_for_commit(transaction_id, lock_keys, lock)
    
@app.route('/concord_home', methods = ['GET'])
def concord_home():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    remote_ip = data['remote_ip']
    key = data['key']
    workflow = data['workflow']
    transaction_id = data['transaction_id']
    value = ''
    if mode == 'invalidated':
        # invalidate the cache on this node.
        value, state = dispatcher.concord_cache_agent[workflow].invalidate_by_home(key, transaction_id)
    if mode == 'read':
        value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_read(transaction_id, key, remote_ip)
    else:
        value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_write(transaction_id, key, remote_ip, mode)
    return json.dumps({'value': value, 'state': state})

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
    if sum([int(config.BASIC), int(config.REPAIR), int(config.REMOTE_LOCK)]) != 1:
        raise Exception("Exactly one of BASIC, REPAIR, or REMOTE_LOCK must be true.")
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   