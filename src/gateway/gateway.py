import json
import gevent
from gevent import monkey
import uuid
import sys
import logging
import re
import gc
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

def gc_loop():
    while True:
        gevent.sleep(300)
        gc.collect()

monkey.patch_all()
from flask import Flask, request
from gateway_repo import Repository
from transaction_info import RunningTXTable
import requests
import gevent.lock
import time

sys.path.append('../../config')
import config

CLEAR_MEM = config.CLEAR_MEM

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable()

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

workflow_metadata  =  {}
workflow_metadata_lock = gevent.lock.BoundedSemaphore()
def get_workflow_metadata(workflow_name):
    with workflow_metadata_lock:
        if workflow_name not in workflow_metadata:
            workflow_metadata[workflow_name] = {'start_functions':[], 'function_ip':{}, 'all_addrs':[]}
            workflow_metadata[workflow_name]['start_functions'] = repo.get_start_functions(workflow_name + '_workflow_metadata')
            for func in workflow_metadata[workflow_name]['start_functions']:
                info = repo.get_function_info(func, workflow_name + '_function_info')
            workflow_metadata[workflow_name]['function_ip'][func] = info['ip']
        workflow_metadata[workflow_name]['all_addrs'] = repo.get_all_addrs(workflow_name + '_workflow_metadata')
    return workflow_metadata[workflow_name]

def trigger_function(workflow_name, transaction_id, function_name, ip, retry, term):
    url = 'http://{}/request'.format(ip)
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'function_name': function_name,
        'no_parent_execution': True,
        'retry':retry,
        'term':term
    }
    requests.post(url, json=data)

def clear_mem(ip, transaction_id, workflow_name):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    requests.post(clear_url, json={'transaction_id': transaction_id, 'workflow_name': workflow_name, 'clear':True})

def reset_on_worker(workflow_name, transaction_id, node_list, term):
    reset_jobs = []
    for ip in node_list:
        pure_ip = extract_ip(ip)
        cache_url = 'http://{}:6000/reset'.format(pure_ip)
        state_url = 'http://{}:7500/clear'.format(pure_ip)
        reset_jobs.append(gevent.spawn(requests.post, cache_url, json={'workflow':workflow_name, 'term':term,'transaction_id':transaction_id}))
        reset_jobs.append(gevent.spawn(requests.post, state_url, json={'workflow_name':workflow_name, 'transaction_id':transaction_id, 'clear':False}))
    gevent.joinall(reset_jobs)  

def run_workflow(workflow_name, workflow_metadata, transaction_id, parameters, retry, term):
    if not retry:
        repo.create_request_doc(transaction_id)
    # allocate works
    start_functions = workflow_metadata['start_functions']
    start = time.time()
    jobs = []
    # logging.info(f"[RUNNING] send req of {transaction_id}, retry: {retry}")
    if type(parameters) is not dict:
        parameters = json.loads(parameters)
    for n in start_functions:
        ip = workflow_metadata['function_ip'][n]
        func_param = parameters.get(n, {})
        if not retry:
            repo.store_input(transaction_id, ip, func_param)
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, n, ip, retry, term))
    gevent.joinall(jobs)
    end = time.time()
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    request_start = time.time()
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = str(uuid.uuid4())
    term = 0
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(workflow)
    # logging.info('processing request ' + transaction_id + '...')
    aborted = False
    abort_type = ''
    retry = False
    term = 0
    node_list = workflow_metadata['all_addrs']
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id) or aborted:
        running_start = time.time()
        exec_first_latency = run_workflow(workflow,workflow_metadata, transaction_id, parameters, retry, term)
        aborted, abort_type = txTable.waitTX(transaction_id)
        if aborted:
            term += 1
            txTable.resetTX(transaction_id, term)
            reset_on_worker(workflow, transaction_id, node_list, term)
            if abort_type == 'PASSIVE':
                retry = True
            else:
                break
    request_end = time.time()
    if aborted and abort_type == 'ACTIVE':
        message =  json.dumps({'status':'aborted', "res": {}})
    # logging.info(f"transaction {transaction_id} latency in the first run: {exec_first_latency}"
    else:
        res = repo.get_result(transaction_id, workflow)
        success_term, commit_latency = txTable.finishTX(transaction_id)
        success_run_latency = (request_end - running_start) - commit_latency
        e2e_latency = request_end - request_start
        message = json.dumps({'status': 'ok', 'e2e_latency': e2e_latency, 'success_run_latency': success_run_latency, 'commit_latency':commit_latency, 'transaction_id': transaction_id, 'rounds':term+1, "res": res})
        # clear memory and other stuff
    if config.CLEAR_MEM:
        worker_addrs = workflow_metadata['all_addrs']
        jobs = []
        # logging.info(f"clearing shadow table and transaction state on {worker_addrs}")
        for ip in worker_addrs:
            jobs.append(gevent.spawn(clear_mem, ip, transaction_id, workflow))
        gevent.joinall(jobs)
    end = time.time()
    # logging.info(f"transaction {transaction_id} finished. e2e_latency: {end-start}, validate_latency: {validate_latency}")
    return message


@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    term = data['term']
    commit_latency = data['commit_latency']
    if data.get('abort', False):
        Abort_type = data.get('Abort_type', 'ACTIVE')
        # logging.info(f"transaction {transaction_id_list[0]} aborted.")
        txTable.notifyTX(transaction_id, term, commit_latency, True, Abort_type)
    else:
        end_time = time.time()
        txTable.notifyTX(transaction_id, term,commit_latency, False, '')  
    return json.dumps({"status": "notified"})

@app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_all_addrs(workflow + '_workflow_metadata')
    jobs = []
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(requests.get, clear_url))
    gevent.joinall(jobs)
    return json.dumps({'status': 'ok'})
    

from gevent.pywsgi import WSGIServer
import logging

# python gateway.py 10.2.29.142 8000
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    gevent.spawn(gc_loop)
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()