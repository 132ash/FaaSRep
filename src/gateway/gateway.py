import json
import gevent
from gevent import monkey
import gevent.lock
import uuid
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

monkey.patch_all()
from flask import Flask, request
from gateway_repo import Repository
from transaction_info import RunningTXTable
import requests
import time

sys.path.append('../../config')
import config

CLEAR_MEM = config.CLEAR_MEM

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable()

workflow_metadata  =  {}
metadata_lock = gevent.lock.BoundedSemaphore()

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
    requests.post(url, json=data)

def clear_mem(ip, transaction_id, workflow_name, abort=False):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    requests.post(clear_url, json={'transaction_id': transaction_id, 'workflow_name': workflow_name, 'abort': abort})

def run_workflow(workflow_name, workflow_metadata, transaction_id, parameters, retry=False):
    if not retry:
        repo.create_request_doc(transaction_id)
    # allocate works
    start_functions = workflow_metadata['start_functions']
    start = time.time()
    jobs = []
    for n in start_functions:
        ip = workflow_metadata['function_ip'][n]
        func_param = parameters.get(n, {})
        if not retry:
            repo.store_input(transaction_id, ip, func_param)
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, n, ip, retry))
    gevent.joinall(jobs)
    end = time.time()
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = str(uuid.uuid4())
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(workflow)
    # logging.info(f'processing request {transaction_id} ..., function_ip:{workflow_metadata["function_ip"]}')
    start = time.time()
    aborted = False
    retry = False
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id) or aborted:
        exec_first_latency = run_workflow(workflow,workflow_metadata, transaction_id, parameters, retry)
        aborted = txTable.waitTX(transaction_id)
        if aborted:
            # logging.info(f"[ABORT] transaction {transaction_id} aborted, clear state and retrying...")
            txTable.resetTX(transaction_id)
            clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
            gevent.joinall(clear_jobs)
        retry = True
    res = repo.get_result(transaction_id, workflow)
    first_run_finish_time, validate_latency,validate_time_inside_validator = txTable.finishTX(transaction_id)
    end = time.time()
    first_run_latency = first_run_finish_time - start
    # logging.info(f"[FINISHED] transaction {transaction_id} finished. e2e_latency: {end-start}, validate_latency: {validate_latency}")
        # clear memory and other stuff
    if config.CLEAR_MEM:
        worker_addrs = workflow_metadata['all_addrs']
        jobs = []
        for ip in worker_addrs:
            jobs.append(gevent.spawn(clear_mem, ip, transaction_id, workflow))
        gevent.joinall(jobs)
    
    return json.dumps({'status': 'ok', 'e2e_latency': end-start, 'first_run_latency':first_run_latency, 'validate_latency': validate_latency,'transaction_id': transaction_id, "res": res, 'validate_time_inside_validator':validate_time_inside_validator})



@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id_lists = data['transaction_id_lists']
    timestamps = data['timestamps']
    aborted_txs_from_validator = data.get('aborted_txs', [])
    if aborted_txs_from_validator:
        txTable.notifyTX(aborted_txs_from_validator, 0, 0, 0, True)
    for transaction_id_list, timestamp_per_batch in zip(transaction_id_lists, timestamps):
        if data.get('abort', False):
            txTable.notifyTX(transaction_id_list, 0,0, 0, True)
        else:
            first_run_finish_time, validate_start_time, validate_time_inside_validator = timestamp_per_batch
            end_time = time.time()
            txTable.notifyTX(transaction_id_list, first_run_finish_time, end_time - validate_start_time, validate_time_inside_validator)  
    return json.dumps({"status": "notified"})

@app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_all_addrs(workflow + '_workflow_metadata')
    jobs = []
    print("clearing containers...")
    print(addrs)
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(requests.get, clear_url))
    gevent.joinall(jobs)
    return json.dumps({'status': 'ok'})

from gevent.pywsgi import WSGIServer
import logging

# python3 gateway.py 10.2.27.24 8000
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()