import json
import gevent
from gevent import monkey
import gevent.lock
import uuid
import sys
import os
import logging
from pathlib import Path
# 配置日志记录
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
LOG_DIR = ROOT_DIR / 'logging'
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / 'gateway.log'

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
import random

import config

CLEAR_MEM = config.CLEAR_MEM
CACHE_ENABLED = config.CACHE_ENABLED

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable()

workflow_metadata = {}
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

def trigger_function(workflow_name, transaction_id, function_name, ip, retry, term=0, birth_seq=None):
    url = 'http://{}/request'.format(ip)
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'function_name': function_name,
        'no_parent_execution': True,
        'repair': False,
        'retry': retry
    }
    if config.SYSTEM_MODE == 'BOKI_SN':
        data.update({'term': term, 'birth_seq': birth_seq})
    ##log_message(f"Triggering function {function_name} for transaction {transaction_id} at {ip}")
    requests.post(url, json=data)

def clear_mem(ip, transaction_id, workflow_name, fin, term=0):
    if not ip.endswith(':7500'):
        ip += ':7500'
    clear_url = 'http://{}/clear'.format(ip)
    requests.post(clear_url, json={'transaction_id': transaction_id, 'workflow_name': workflow_name, 'fin': fin, 'term': term})

def run_workflow(workflow_name, workflow_metadata, transaction_id, parameters, retry=False, term=0, birth_seq=None):
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
        if not retry or config.SYSTEM_MODE == 'BOKI_SN':
            repo.store_input(transaction_id, ip, func_param, term)
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, n, ip, retry, term, birth_seq))
    gevent.joinall(jobs)
    end = time.time()
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = data.get('transaction_id', str(uuid.uuid4()))
    global_req_id = data.get('global_req_id')
    if config.SYSTEM_MODE == 'BOKI_SN':
        try:
            global_req_id = int(global_req_id)
            if global_req_id < 0:
                raise ValueError
        except (TypeError, ValueError):
            return json.dumps({'status': 'error', 'transaction_id': transaction_id,
                               'error': 'BOKI_SN requires a non-negative integer global_req_id'}), 400
    txTable.registerTX(workflow, transaction_id, parameters)
    workflow_metadata = get_workflow_metadata(workflow)
    if config.SYSTEM_MODE == 'BOKI_SN':
        return run_boki(workflow, workflow_metadata, transaction_id, parameters, global_req_id)
    #log_message(f'processing request {transaction_id}')
    start = time.time()
    aborted = False
    retry = False
    active_abort=False
    workflow_exec_latency=0
    validate_latency=0
    rounds=0
    success_exec_latency=0
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id) or aborted:
        running_start = time.time()
        run_workflow(workflow,workflow_metadata, transaction_id, parameters, retry)
        aborted, active_abort, finish_time, validate_time = txTable.waitTX(transaction_id)
        workflow_exec_latency += (finish_time - running_start)
        validate_latency += validate_time
        if aborted:
            #log_message(f"[ABORT] transaction {transaction_id} aborted, active_abort: {active_abort}")
            if not active_abort:
                clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, False) for ip in workflow_metadata['all_addrs']]
                gevent.joinall(clear_jobs)
                txTable.resetTX(transaction_id)
                rounds += 1
            else:
                break
        else:
            success_exec_latency = finish_time - running_start
        retry = True
    end = time.time()
    if aborted and active_abort: 
        message = json.dumps({'status':'aborted', "res": {}, 'transaction_id':transaction_id})
    else:
        #log_message(f"[FINISH] transaction {transaction_id} finished, e2e latency: {end-start}")
        res = repo.get_result(transaction_id, workflow)
        commit_latency = txTable.finishTX(transaction_id)
        message = json.dumps({'status': 'ok', 'e2e_latency': end-start, 'workflow_exec_latency':workflow_exec_latency, 'validate_latency': validate_latency,'transaction_id': transaction_id, "res": res, 'commit_latency': commit_latency, 'rounds':rounds+1, 'success_exec_latency':success_exec_latency})
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
        gevent.joinall(clear_jobs)
        repo.clear_db(transaction_id)
    return message


def _boki_post(addr, path, payload, timeout=35):
    response = requests.post(f'http://{addr}{path}', json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _boki_emergency_abort(transaction_id, term, reason):
    """Best-effort timeout/error cleanup without violating flush atomicity.

    A FLUSHING attempt intentionally cannot be discarded; it keeps its locks so
    a partial database write is never exposed.  ACTIVE attempts are discarded
    before the corresponding lock release.
    """
    try:
        discarded = _boki_post(config.SHADOW_SERVICE_ADDR, '/discard', {
            'txid': transaction_id, 'term': term, 'reason': reason})
        if discarded.get('status') == 'DISCARDED':
            _boki_post(config.LOCK_MANAGER_ADDR, '/abort', {
                'txid': transaction_id, 'term': term, 'abort_type': reason})
    except Exception:
        logging.exception('unable to clean up Boki attempt %s/%s', transaction_id, term)


def _boki_retry_backoff_seconds():
    """Return a jittered retry delay while preserving the configured mean.

    With the default base of 0.2 and ratio 0.5 this is uniformly sampled from
    0.1 to 0.3 seconds.  A positive base is required: zero-delay retries
    synchronize contenders into a hot-key retry storm.
    """
    base = max(0.0, float(getattr(config, 'BOKI_RETRY_BACKOFF_SECONDS', 0.2)))
    ratio = max(0.0, float(getattr(config, 'BOKI_RETRY_BACKOFF_JITTER_RATIO', 0.5)))
    if base == 0:
        return 0.0
    return random.uniform(base * max(0.0, 1.0 - ratio), base * (1.0 + ratio))


def run_boki(workflow, workflow_metadata, transaction_id, parameters, global_req_id):
    """Gateway-owned retry loop: term advances, birth priority never does."""
    start = time.time()
    term = 0
    birth_seq = None
    retry_count = 0
    wait_die_abort_count = 0
    last_metrics = {}
    final_status = 'error'
    error = None
    while True:
        attempt_started = False
        try:
            begun = _boki_post(config.LOCK_MANAGER_ADDR, '/begin', {
                'txid': transaction_id, 'term': term, 'global_req_id': global_req_id})
            if begun.get('status') != 'ACTIVE':
                raise RuntimeError(f'lock begin: {begun}')
            birth_seq = begun['birth_seq']
            shadow = _boki_post(config.SHADOW_SERVICE_ADDR, '/begin', {
                'txid': transaction_id, 'term': term, 'birth_seq': birth_seq})
            if shadow.get('status') != 'ACTIVE':
                _boki_post(config.LOCK_MANAGER_ADDR, '/abort', {'txid': transaction_id, 'term': term, 'abort_type': 'BEGIN_ERROR'})
                raise RuntimeError(f'shadow begin: {shadow}')
            attempt_started = True
            txTable.set_boki_attempt(transaction_id, term, birth_seq)
            running_start = time.time()
            run_workflow(workflow, workflow_metadata, transaction_id, parameters, retry_count > 0, term, birth_seq)
            outcome = txTable.waitBoki(transaction_id, getattr(config, 'BOKI_WORKFLOW_WAIT_SECONDS', 120))
            last_metrics = outcome.get('metrics', {})
            if outcome['status'] == 'committed':
                final_status = 'ok'
                last_metrics['workflow_exec_latency'] = outcome['finish_time'] - running_start
                break
            if outcome['status'] == 'aborted' and outcome.get('abort_type') in {'PASSIVE', 'WAIT_DIE', 'TIMEOUT'}:
                wait_die_abort_count += 1
                retry_count += 1
                clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, False, term)
                              for ip in workflow_metadata['all_addrs']]
                gevent.joinall(clear_jobs)
                term += 1
                # gevent.sleep(_boki_retry_backoff_seconds())
                continue
            if outcome['status'] == 'aborted':
                if outcome.get('abort_type') == 'ERROR':
                    error = outcome.get('error') or 'application or container error'
                else:
                    final_status = 'aborted'
            else:
                error = outcome.get('error')
                _boki_emergency_abort(transaction_id, term, 'TIMEOUT_OR_ERROR')
            break
        except Exception as exc:
            error = str(exc)
            if attempt_started:
                _boki_emergency_abort(transaction_id, term, 'GATEWAY_ERROR')
            break
    end = time.time()
    if final_status == 'ok':
        res = repo.get_result(transaction_id, workflow, term)
        message = {'status': 'ok', 'transaction_id': transaction_id, 'global_req_id': global_req_id, 'res': res,
                   'e2e_latency': end - start, 'workflow_exec_latency': last_metrics.get('workflow_exec_latency', 0),
                   'rounds': retry_count + 1, 'retry_count': retry_count,
                   'wait_die_abort_count': wait_die_abort_count, 'term': term, **last_metrics}
    elif final_status == 'aborted':
        message = {'status': 'aborted', 'transaction_id': transaction_id, 'global_req_id': global_req_id, 'res': {},
                   'rounds': retry_count + 1, 'term': term}
    else:
        message = {'status': 'error', 'transaction_id': transaction_id, 'global_req_id': global_req_id,
                   'error': error or 'Boki attempt failed',
                   'rounds': retry_count + 1, 'term': term}
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True, term)
                      for ip in workflow_metadata['all_addrs']]
        gevent.joinall(clear_jobs)
        repo.clear_db(transaction_id)
    txTable.finishTX(transaction_id)
    return json.dumps(message)

@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    if config.SYSTEM_MODE == 'BOKI_SN':
        accepted = txTable.notifyBoki(data['txid'], int(data['term']), data['status'],
                                      data.get('abort_type'), data.get('metrics'), data.get('error'))
        return json.dumps({'status': 'notified' if accepted else 'stale'})
    from_validator = data.get('from_validator', False)
    # abort or commited from validator
    if from_validator:
        aborted_txs_from_validator = data.get('aborted_txs', [])
        commited_txs_from_validator = data.get('commited_txs', [])
        validate_time, commit_time = data['timestamps']
        txTable.notifyTX(commited_txs_from_validator, aborted_txs_from_validator, validate_time, commit_time, False)
    else:
        txTable.notifyTX([], data.get('aborted_txs', []), 0, 0, True)  # this is for the case when the transaction is aborted by the app itself.
    return json.dumps({"status": "notified"})

@app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_all_addrs(workflow + '_workflow_metadata')
    jobs = []
    # print("clearing containers...")
    # print(addrs)
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(requests.get, clear_url))
    gevent.joinall(jobs)
    return json.dumps({'status': 'ok'})

from gevent.pywsgi import WSGIServer
import logging

#  python gateway.py  10.2.29.142  8000
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
