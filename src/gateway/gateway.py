import json
import gevent
from gevent import monkey
import uuid
import sys
import logging
import os
# 配置日志记录
log_file = '../../logging/gateway.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

def setup_logger():
    logger = logging.getLogger('gateway')
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

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()

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
txTable = RunningTXTable()

workflow_metadata  =  {}
workflow_metadata_lock = gevent.lock.BoundedSemaphore()
def get_workflow_metadata(repo, workflow_name):
    with workflow_metadata_lock:
        if workflow_name not in workflow_metadata:
            workflow_metadata[workflow_name] = {'start_functions':[], 'function_ip':{}, 'all_addrs':[]}
            workflow_metadata[workflow_name]['start_functions'] = repo.get_start_functions(workflow_name + '_workflow_metadata')
            for func in workflow_metadata[workflow_name]['start_functions']:
                info = repo.get_function_info(func, workflow_name + '_function_info')
            workflow_metadata[workflow_name]['function_ip'][func] = info['ip']
        workflow_metadata[workflow_name]['all_addrs'] = repo.get_all_addrs(workflow_name + '_workflow_metadata')
    return workflow_metadata[workflow_name]

def trigger_function(create_timestamp, workflow_name, transaction_id, function_name, ip, retry, term):
    url = 'http://{}/request'.format(ip)
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'function_name': function_name,
        'no_parent_execution': True,
        'create_timestamp':create_timestamp,
        'retry':retry,
        'term':term
    }
    requests.post(url, json=data)

def clear_mem(ip, transaction_id, workflow_name):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    requests.post(clear_url, json={'transaction_id': transaction_id, 'workflow_name': workflow_name, 'fin':True})


def run_workflow(repo, create_timestamp, workflow_name, workflow_metadata, transaction_id, parameters, retry, term):
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
        jobs.append(gevent.spawn(trigger_function, create_timestamp, workflow_name, transaction_id, n, ip, retry, term))
    gevent.joinall(jobs)
    end = time.time()
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    repo = Repository()
    request_start = time.time()
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = str(uuid.uuid4())
    term = 0
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(repo, workflow)
    repo.create_shadow_table(transaction_id)
    #log_message('processing request ' + transaction_id + '...')
    aborted = False
    abort_type = ''
    retry = False
    term = 0
    workflow_exec_latency=0
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id) or aborted:
        running_start = time.time()
        workflow_exec_latency += run_workflow(repo, time.time(), workflow,workflow_metadata, transaction_id, parameters, retry, term)
        aborted, abort_type = txTable.waitTX(transaction_id)
        if aborted:
            #log_message(f"transaction {transaction_id} aborted, term: {term}, retry: {retry}, abort_type: {abort_type}")
            repo.reset_and_release_locks_for_retry(transaction_id)
            if abort_type == 'PASSIVE':
                retry = True
                term += 1
                txTable.resetTX(transaction_id, term)
            else:
                break
    request_end = time.time()
    if aborted and abort_type == 'ACTIVE':
        message =  json.dumps({'status':'aborted', "res": {}})
    # logging.info(f"transaction {transaction_id} latency in the first run: {exec_first_latency}"
    else:
        res = repo.get_result(transaction_id, workflow)
        repo.release_all_locks(transaction_id)
        success_term, commit_latency = txTable.finishTX(transaction_id)
        e2e_latency = request_end - request_start
        message = json.dumps({'status': 'ok', 'e2e_latency': e2e_latency, 'workflow_exec_latency': workflow_exec_latency, 'commit_latency':commit_latency, 'transaction_id': transaction_id, 'rounds':term, "res": res})
        # clear memory and other stuff
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow) for ip in workflow_metadata['all_addrs']]
        gevent.joinall(clear_jobs)
        repo.clear_db(transaction_id)
    end = time.time()
    #log_message(f"transaction {transaction_id} finished.")
    return message


@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    term = data['term']
    commit_latency = data['commit_latency']
    if data.get('abort', False):
        Abort_type = data.get('Abort_type', 'ACTIVE')
        #log_message(f"[ABORT ACCEPTED] transaction {transaction_id} aborted, term: {term}, commit_latency: {commit_latency}, Abort_type: {Abort_type}")
        txTable.notifyTX(transaction_id, term, commit_latency, True, Abort_type)
    else:
        txTable.notifyTX(transaction_id, term,commit_latency, False, '')  
    return json.dumps({"status": "notified"})

from gevent.pywsgi import WSGIServer
import logging

# python gateway.py 10.2.29.142 8000
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    repo = Repository()
    repo.clear_db()
    server.serve_forever()