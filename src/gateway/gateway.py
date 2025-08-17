import json
import gevent
from gevent import monkey
import uuid
import sys
import os
import logging
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
import time
import gevent.lock

session = requests.Session()

sys.path.append('../../config')
import config

CLEAR_MEM = config.CLEAR_MEM

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable(repo)
repo.delete_shadow_table()

metadata_lock = gevent.lock.BoundedSemaphore()
workflow_metadata  =  {}
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

def trigger_function(workflow_name, transaction_id, create_timestamp, function_name, ip, retry):
    url = 'http://{}/request'.format(ip)
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'create_timestamp': create_timestamp,
        'function_name': function_name,
        'no_parent_execution': True,
        'repair': False,
        'retry': retry
    }
    session.post(url, json=data)

def clear_mem(ip, transaction_id, workflow_name, abort=False):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    session.post(clear_url, json={'transaction_id': transaction_id, 'workflow_name': workflow_name, 'abort': abort})

def run_workflow(create_timestamp, workflow_name, workflow_metadata, transaction_id, parameters, retry=False):
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
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, create_timestamp, n, ip, retry))
    gevent.joinall(jobs)
    end = time.time()
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = data.get('transaction_id', str(uuid.uuid4()))
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(workflow)
    ## logging.info('processing request ' + transaction_id + '...')
    start = time.time()
    repo.create_shadow_table(transaction_id)
    aborted = False
    Abort_type = ''
    retry = False
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id) or aborted:
        # logging.info(f"running workflow {workflow}, transaction_id: {transaction_id}, retry: {retry}")
        exec_first_latency = run_workflow(time.time(), workflow, workflow_metadata, transaction_id, parameters, retry)
        aborted, Abort_type = txTable.waitTX(transaction_id)
        if aborted:
            clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
            gevent.joinall(clear_jobs)
            if Abort_type == 'PASSIVE':
                txTable.resetTX(transaction_id)           
            else:
                break
        retry = True
    if aborted and Abort_type == 'ACTIVE':
        return json.dumps({'status':'aborted', "res": {}})
    ## logging.info(f"transaction {transaction_id} latency in the first run: {exec_first_latency}")
    res = repo.get_result(transaction_id, workflow)
    first_run_finish_time, validate_latency, validate_time_inside_validator = txTable.finishTX(transaction_id)
    end = time.time()
    first_run_latency = first_run_finish_time - start
    # log_message(f"transaction {transaction_id} finished. e2e_latency: {end-start}, res: {res}")
    #     # clear memory and other stuff
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow) for ip in workflow_metadata['all_addrs']]
        gevent.joinall(clear_jobs)
        repo.clear_db(transaction_id)

    return json.dumps({'status': 'ok', 'e2e_latency': end-start, 'first_run_latency':first_run_latency, 'validate_latency': validate_latency,'transaction_id': transaction_id, "res": res, 'validate_time_inside_validator':validate_time_inside_validator})



@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    first_run_finish_time = data['first_run_finish_time']
    
    if data.get('abort', False):
        Abort_type = data['Abort_type']
        # log_message(f"transaction {transaction_id} aborted. Abort_type:{Abort_type}")
        txTable.notifyTX(transaction_id, 0, True, Abort_type)
    else:
        first_run_finish_time = data['first_run_finish_time']
        txTable.notifyTX(transaction_id, first_run_finish_time)  
    return json.dumps({"status": "notified"})

@app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_all_addrs(workflow + '_workflow_metadata')
    jobs = []
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(session.get, clear_url))
    gevent.joinall(jobs)
    return json.dumps({'status': 'ok'})

from gevent.pywsgi import WSGIServer
import logging

# python3 gateway.py  10.2.27.22 8000
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()