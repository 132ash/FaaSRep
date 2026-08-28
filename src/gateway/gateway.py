import json
import gevent
from gevent import monkey
import gevent.lock
import uuid
import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / 'config'))
from experiment_logging import make_experiment_logger, configure_root_experiment_logging

def setup_logger():
    configure_root_experiment_logging('gateway_runtime')
    return make_experiment_logger('gateway', 'gateway')

# 全局logger实例
logger = setup_logger()

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()
monkey.patch_all()
from flask import Flask, request
from gateway_repo import Repository
from transaction_info import (
    RunningTXTable,
    is_injected_retry_abort,
    prepare_occ_retry_parameters,
)
import requests
import time

sys.path.append('../../config')
import config

CLEAR_MEM = config.CLEAR_MEM

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable()

def report_gateway_progress():
    snapshot = {
        tx_id: {
            'workflow': state['workflow'],
            'finished': state['finished'],
            'abort': state['abort'],
            'pessimistic': state['pessimistic'],
        }
        for tx_id, state in txTable.running_txs.items()
    }
    log_message(json.dumps({
        'event': 'GATEWAY_PROGRESS_SNAPSHOT', 'workflow': '',
        'batch_id': '', 'tx_id': '', 'function': '', 'repair_mode': '',
        'repair_epoch': 0, 'attempt_id': '', 'state_before': '',
        'state_after': '', 'active_transactions': snapshot,
        'last_transition_timestamp': txTable.last_transition_timestamp,
        'timestamp': time.time(),
    }, sort_keys=True))
    gevent.spawn_later(10, report_gateway_progress)

gevent.spawn_later(10, report_gateway_progress)

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
        # Transaction cleanup only needs workers that host this workflow.
        # The workflow metadata contains every configured worker, including
        # nodes that did not participate and have no state to clear.
        workflow_metadata[workflow_name]['all_addrs'] = \
            repo.get_function_addrs(workflow_name)
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
    #log_message(f"Triggering function {function_name} for transaction {transaction_id} at {ip}")
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
    if type(parameters) is not dict:
        parameters = json.loads(parameters)
    for n in start_functions:
        ip = workflow_metadata['function_ip'][n]
        func_param = parameters.get(n, {})
        # OCC retry cleanup removes the old request-local input as well, so
        # every execution attempt must restore the start-function input.
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
    transaction_id = data.get('transaction_id', str(uuid.uuid4()))
    txTable.registerTX(workflow, transaction_id, parameters)
    log_message(json.dumps({
        'event': 'TX_REGISTER', 'workflow': workflow, 'batch_id': '',
        'tx_id': transaction_id, 'function': '', 'repair_mode': '',
        'repair_epoch': 0, 'attempt_id': '', 'state_before': '',
        'state_after': 'RUNNING', 'timestamp': time.time(),
    }, sort_keys=True))
    workflow_metadata = get_workflow_metadata(workflow)
    #log_message(f'processing request {transaction_id} ..., function_ip:{workflow_metadata["function_ip"]}')
    start = time.time()
    aborted = False
    retry = False
    occ_retries = 0
    # run the workflow,  the workflow may abort in the middle.
    while not txTable.TxFinished(transaction_id):
        exec_first_run_latency = run_workflow(workflow,workflow_metadata, transaction_id, parameters, retry)
        log_message(json.dumps({
            'event': 'WAIT_TX_BEGIN', 'workflow': workflow, 'batch_id': '',
            'tx_id': transaction_id, 'function': '', 'repair_mode': '',
            'repair_epoch': 0, 'attempt_id': '', 'state_before': 'RUNNING',
            'state_after': 'WAITING', 'timestamp': time.time(),
        }, sort_keys=True))
        aborted = txTable.waitTX(transaction_id)
        abort_error = txTable.running_txs[transaction_id].get('error', '')
        injected_abort = is_injected_retry_abort(aborted, abort_error)
        if txTable.retryRequested(transaction_id) or injected_abort:
            log_message(json.dumps({
                'event': 'OCC_RETRY_BEGIN', 'workflow': workflow,
                'batch_id': '', 'tx_id': transaction_id, 'function': '',
                'repair_mode': '', 'repair_epoch': 0, 'attempt_id': '',
                'state_before': (
                    'INJECTED_ABORT' if injected_abort else 'VALIDATION_ABORT'
                ),
                'state_after': 'RETRYING', 'timestamp': time.time(),
            }, sort_keys=True))
            clear_jobs = [
                gevent.spawn(clear_mem, ip, transaction_id, workflow, False)
                for ip in workflow_metadata['all_addrs']
            ]
            gevent.joinall(clear_jobs)
            txTable.resetTX(transaction_id)
            parameters = prepare_occ_retry_parameters(
                parameters, workflow_metadata['start_functions'])
            retry = True
            occ_retries += 1
            aborted = False
    #log_message(f"transaction {transaction_id} in {workflow}  finished running, checking finished or aborted...")
    if aborted:
        abort_error = txTable.running_txs[transaction_id].get('error', '')
        message = json.dumps({'status':'aborted', "res": {}, 'transaction_id':transaction_id,
                              'e2e_latency': time.time() - start, 'rounds': 2,
                              'occ_retries': occ_retries,
                              'error': abort_error})
        log_message(json.dumps({
            'event': 'TX_TERMINAL_ABORT', 'workflow': workflow, 'batch_id': '',
            'tx_id': transaction_id, 'function': '', 'repair_mode': '',
            'repair_epoch': 0, 'attempt_id': '', 'state_before': 'WAITING',
            'state_after': 'ABORTED', 'timestamp': time.time(),
        }, sort_keys=True))
        txTable.running_txs.pop(transaction_id, None)
    else:
        first_run_finish_time, repair_start_time, repair_finish_time, commit_finish_time, notify_received_time, pessimistic = txTable.finishTX(transaction_id)
        result_fetch_start = time.time()
        res = repo.get_result(transaction_id, workflow)
        end = time.time()
        first_run_latency = first_run_finish_time - start
        time_inside_validator = repair_start_time - first_run_finish_time
        time_repair = repair_finish_time - repair_start_time
        time_commit = commit_finish_time - repair_finish_time
        result_fetch_latency = end - result_fetch_start
        post_commit_gateway_latency = end - commit_finish_time
        notify_to_fetch_start_latency = result_fetch_start - notify_received_time
        rounds = 3 if pessimistic else 2
        message = json.dumps({
            'status': 'ok',
            'e2e_latency': end-start,
            'workflow_exec_latency':first_run_latency,
            'transaction_id': transaction_id,
            "res": res,
            'time_inside_validator':time_inside_validator,
            'time_repair':time_repair,
            'time_commit':time_commit,
            'result_fetch_latency': result_fetch_latency,
            'post_commit_gateway_latency': post_commit_gateway_latency,
            'notify_to_fetch_start_latency': notify_to_fetch_start_latency,
            'rounds': rounds,
            'occ_retries': occ_retries,
        })
        log_message(json.dumps({
            'event': 'TX_TERMINAL_COMMIT', 'workflow': workflow, 'batch_id': '',
            'tx_id': transaction_id, 'function': '', 'repair_mode': '',
            'repair_epoch': 0, 'attempt_id': '', 'state_before': 'WAITING',
            'state_after': 'COMMITTED', 'timestamp': time.time(),
        }, sort_keys=True))
    #log_message(f"transaction {transaction_id} in {workflow} aborted: {aborted}, clearing states")
    if config.CLEAR_MEM:
        clear_jobs = [gevent.spawn(clear_mem, ip, transaction_id, workflow, True) for ip in workflow_metadata['all_addrs']]
        gevent.joinall(clear_jobs)
    #log_message(f"transaction {transaction_id}  in {workflow} cleaned, return results")
    return message

@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id_lists = data['transaction_id_lists']
    timestamps = data['timestamps']
    aborted_txs_from_validator = data.get('aborted_txs', [])
    aborted_errors = data.get('aborted_errors', {})
    retry_txs = data.get('retry_txs', [])
    pessimistic_txs = data.get('pessimistic_txs', [])
    log_message(json.dumps({
        'event': 'NOTIFY_RECEIVED', 'workflow': '', 'batch_id': '',
        'tx_id': '', 'function': '', 'repair_mode': '', 'repair_epoch': 0,
        'attempt_id': '', 'state_before': '', 'state_after': 'RECEIVED',
        'transaction_count': sum(len(items) for items in transaction_id_lists),
        'timestamp': time.time(),
    }, sort_keys=True))
    #log_message(f"notify txs, aborted_txs_from_validator:{aborted_txs_from_validator}, successed_transaction_id_lists:{transaction_id_lists}, timestamps:{timestamps}, abort:{data.get('abort', False)}")
    #log_message(f'notify, running_txs:{list(txTable.running_txs.keys())}')
    if aborted_txs_from_validator:
        txTable.notifyTX(aborted_txs_from_validator, 0, 0, 0, abort=True,
                         pessimistic_txs={}, abort_errors=aborted_errors)
    if retry_txs:
        txTable.notifyRetry(retry_txs)
    for transaction_id_list, timestamp_per_batch, pessimistic_txs_per_batch in zip(transaction_id_lists, timestamps, pessimistic_txs):
        if data.get('abort', False):
            txTable.notifyTX(transaction_id_list, 0,0, 0, abort=True, pessimistic_txs={})
        else:
            first_run_finish_time, repair_start_time, repair_finish_time = timestamp_per_batch[0], timestamp_per_batch[1], timestamp_per_batch[2]
            commit_finish_time = timestamp_per_batch[3] if len(timestamp_per_batch) > 3 else repair_finish_time
            txTable.notifyTX(transaction_id_list, first_run_finish_time, repair_start_time, repair_finish_time, commit_finish_time, time.time(), False, pessimistic_txs_per_batch)  
    return json.dumps({"status": "notified"})

@app.route('/clear_container', methods = ['POST'])
def clear_container():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    addrs = repo.get_function_addrs(workflow)
    jobs = []
    log_message(json.dumps({
        'event': 'CLEAR_BEGIN', 'workflow': workflow,
        'worker_addresses': addrs, 'timestamp': time.time(),
    }, sort_keys=True))
    for addr in addrs:
        clear_url = f'http://{addr}/clear_container'
        jobs.append(gevent.spawn(requests.get, clear_url))
    gevent.joinall(jobs)
    return json.dumps({'status': 'ok'})

from gevent.pywsgi import WSGIServer
import logging

#  python gateway.py  10.2.29.142  8000
if __name__ == '__main__':
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app,
                        log=None, error_log=None)
    server.serve_forever()
