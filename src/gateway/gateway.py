import json
import gevent
from gevent import monkey
import gevent.lock
import uuid
import sys
import os
import logging
from pathlib import Path

sys.path.append('../../config')
import config
from logging_utils import RunAwareFileHandler

def setup_logger():
    logger = logging.getLogger('gateway')
    logger.setLevel(logging.INFO)
    # 动态跟随当前 run_id 的文件处理器
    file_handler = RunAwareFileHandler(config.ROOT_DIR, 'gateway.log')
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

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()
monkey.patch_all()
from flask import Flask, request
from gateway_repo import Repository
from transaction_info import RunningTXTable
import requests
import time

CLEAR_MEM = config.CLEAR_MEM

public_app = Flask("gateway_public")
notify_app = Flask("gateway_notify")
app = public_app
repo = Repository()
txTable = RunningTXTable()

workflow_metadata = {}
metadata_lock = gevent.lock.BoundedSemaphore()

def post_json(url, data, context):
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        log_message(f"[HTTP ERROR] {context}: {url}: {exc}")
        return None

def get_url(url, context):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        log_message(f"[HTTP ERROR] {context}: {url}: {exc}")
        return None

def join_and_report(jobs, context):
    if not jobs:
        return True
    gevent.joinall(jobs)
    ok = True
    for job in jobs:
        if job.exception is not None:
            ok = False
            log_message(f"[GEVENT ERROR] {context}: {job.exception}")
    return ok

def get_workflow_metadata(workflow_name):
    metadata_lock.acquire()
    if workflow_name not in workflow_metadata:
        workflow_metadata[workflow_name] = {'start_functions':[], 'function_ip':{}, 'all_addrs':[]}
        workflow_metadata[workflow_name]['start_functions'] = repo.get_start_functions(workflow_name + '_workflow_metadata')
        for func in workflow_metadata[workflow_name]['start_functions']:
            info = repo.get_function_info(func, workflow_name + '_function_info')
            workflow_metadata[workflow_name]['function_ip'][func] = info['ip']
        workflow_metadata[workflow_name]['all_addrs'] = repo.get_all_addrs(workflow_name + '_workflow_metadata')
    metadata_lock.release()
    return workflow_metadata[workflow_name]

def trigger_function(workflow_name, transaction_id, function_name, ip, retry):
    url = 'http://{}/request'.format(ip)
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'function_name': function_name,
        'no_parent_execution': True,
        'repair': False,
        'retry': retry
    }
    log_message(f"Triggering function {function_name} for transaction {transaction_id} at {ip}")
    post_json(url, data, f"trigger {transaction_id}/{function_name}")

def clear_mem(ip, transaction_id, workflow_name, abort=False):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    post_json(clear_url, {'transaction_id': transaction_id, 'workflow_name': workflow_name, 'abort': abort}, f"clear mem {transaction_id}")

def run_workflow(workflow_name, workflow_metadata, transaction_id, parameters, retry=False):
    if not retry:
        repo.create_request_doc(transaction_id)
    # allocate works
    start_functions = workflow_metadata['start_functions']
    start = time.time()
    jobs = []
    if type(parameters) is not dict:
        parameters = json.loads(parameters)
    for n in start_functions:
        ip = workflow_metadata['function_ip'][n]
        func_param = parameters.get(n, {})
        if not retry:
            repo.store_input(transaction_id, ip, func_param)
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, n, ip, retry))
    join_and_report(jobs, f"first run trigger {transaction_id}")
    end = time.time()
    return end - start

@public_app.route('/healthz', methods=['GET'])
def public_healthz():
    return json.dumps({"status": "ok", "listener": "public"})


@notify_app.route('/healthz', methods=['GET'])
def notify_healthz():
    return json.dumps({"status": "ok", "listener": "notify"})


@public_app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = data.get('transaction_id', str(uuid.uuid4()))
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(workflow)
    log_message(f'processing request {transaction_id} ..., function_ip:{workflow_metadata["function_ip"]}')
    start = time.time()
    aborted = False
    retry = False
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id):
        exec_first_run_latency = run_workflow(workflow,workflow_metadata, transaction_id, parameters, retry)
        aborted = txTable.waitTX(transaction_id)
        # if aborted:
        #     # clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
        #     # gevent.joinall(clear_jobs)
        #     break
        #     txTable.resetTX(transaction_id)
        # retry = True
    log_message(f"transaction {transaction_id} in {workflow}  finished running, checking finished or aborted...")
    if aborted:
        message = json.dumps({'status':'aborted', "res": {}, 'transaction_id':transaction_id})
        txTable.running_txs.pop(transaction_id, None)
    else:
        res = repo.get_result(transaction_id, workflow)
        first_run_finish_time, repair_start_time, repair_finish_time, pessimistic = txTable.finishTX(transaction_id)
        end = time.time()
        first_run_latency = first_run_finish_time - start
        time_inside_validator = repair_start_time - first_run_finish_time
        time_repair = repair_finish_time - repair_start_time
        time_commit = end - repair_finish_time
        rounds = 3 if pessimistic else 2
        message = json.dumps({'status': 'ok', 'e2e_latency': end-start, 'workflow_exec_latency':first_run_latency, 'transaction_id': transaction_id, "res": res, 'time_inside_validator':time_inside_validator, 'time_repair':time_repair, 'time_commit':time_commit, 'rounds':rounds})
    log_message(f"transaction {transaction_id} in {workflow} aborted: {aborted}, clearing states")
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
        join_and_report(clear_jobs, f"clear transaction {transaction_id}")
    log_message(f"transaction {transaction_id}  in {workflow} cleaned, return results")
    return message

@notify_app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id_lists = data['transaction_id_lists']
    timestamps = data['timestamps']
    aborted_txs_from_validator = data.get('aborted_txs', [])
    pessimistic_txs = data.get('pessimistic_txs', [])
    log_message(f"notify txs, aborted_txs_from_validator:{aborted_txs_from_validator}, successed_transaction_id_lists:{transaction_id_lists}, timestamps:{timestamps}, abort:{data.get('abort', False)}")
    log_message(f'notify, running_txs:{list(txTable.running_txs.keys())}')
    if aborted_txs_from_validator:
        txTable.notifyTX(aborted_txs_from_validator, 0, 0, 0, True, {})
    for transaction_id_list, timestamp_per_batch, pessimistic_txs_per_batch in zip(transaction_id_lists, timestamps, pessimistic_txs):
        if data.get('abort', False):
            txTable.notifyTX(transaction_id_list, 0,0, 0, True, {})
        else:
            first_run_finish_time, repair_start_time, repair_finish_time = timestamp_per_batch[0], timestamp_per_batch[1], timestamp_per_batch[2]
            txTable.notifyTX(transaction_id_list, first_run_finish_time, repair_start_time, repair_finish_time, False, pessimistic_txs_per_batch)  
    return json.dumps({"status": "notified"})

@public_app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_all_addrs(workflow + '_workflow_metadata')
    jobs = []
    print("clearing containers...")
    print(addrs)
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(get_url, clear_url, f"clear container {addr}"))
    join_and_report(jobs, "clear containers")
    return json.dumps({'status': 'ok'})

from gevent.pywsgi import WSGIServer
import logging


def _port_from_addr(addr: str, default: int) -> int:
    try:
        return int(str(addr).rsplit(":", 1)[1])
    except (IndexError, ValueError):
        return default


# python gateway.py 10.2.30.50 8000 8001
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    host = sys.argv[1]
    public_port = int(sys.argv[2])
    notify_port = (
        int(sys.argv[3])
        if len(sys.argv) > 3
        else _port_from_addr(config.GATEWAY_NOTIFY_ADDR, public_port + 1)
    )
    public_server = WSGIServer((host, public_port), public_app, backlog=config.HTTP_SERVER_BACKLOG)
    notify_server = WSGIServer((host, notify_port), notify_app, backlog=config.HTTP_SERVER_BACKLOG)
    log_message(f"gateway public listener on {host}:{public_port}")
    log_message(f"gateway notify listener on {host}:{notify_port}")
    public_server.start()
    notify_server.start()
    gevent.wait()
