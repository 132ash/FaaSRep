import json
import gevent
from gevent import monkey
import uuid
monkey.patch_all()
import sys
from flask import Flask, request
from gateway_repo import Repository
from transaction_info import RunningTXTable
import requests
import time
import logging

sys.path.append('../../config')
import config

app = Flask(__name__)
repo = Repository()
txTable = RunningTXTable()

def trigger_function(workflow_name, transaction_id, function_name, ip):
    url = 'http://{}/request'.format(ip)
    print(f"sending req to {url}")
    data = {
        'transaction_id': transaction_id,
        'workflow_name': workflow_name,
        'function_name': function_name,
        'no_parent_execution': True,
        'repair': False
    }
    requests.post(url, json=data)

def run_workflow(workflow_name, transaction_id, parameters):
    repo.create_request_doc(transaction_id)

    # allocate works
    start_functions = repo.get_start_functions(workflow_name + '_workflow_metadata')
    print(f"start_functions: {start_functions}")
    start = time.time()
    jobs = []
    for n in start_functions:
        info = repo.get_function_info(n, workflow_name + '_function_info')
        ip = info['ip']
        repo.store_input(transaction_id, ip, parameters[n])
        jobs.append(gevent.spawn(trigger_function, workflow_name, transaction_id, n, ip))
    gevent.joinall(jobs)
    end = time.time()

    # clear memory and other stuff
    if config.CLEAR_DB_AND_MEM:
        master_addr = repo.get_all_addrs(workflow_name + '_workflow_metadata')[0]
        clear_url = 'http://{}/clear'.format(master_addr)
        requests.post(clear_url, json={'transaction_id': transaction_id, 'master': True, 'workflow_name': workflow_name})
    
    return end - start

@app.route('/run', methods = ['POST'])
def run():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow']
    parameters = data['parameters']
    transaction_id = str(uuid.uuid4())
    txTable.registerTX(transaction_id, parameters)
    print('processing request ' + transaction_id + '...')
    latency = run_workflow(workflow, transaction_id, parameters)
    txTable.waitTX(transaction_id)
    print('request ' + transaction_id + ' done')
    res = repo.get_result(transaction_id)
    print(f"transaction_id: f{transaction_id}, res: {res}")
    txTable.finishTX(transaction_id)
    return json.dumps({'status': 'ok', 'latency': latency, 'TxID': transaction_id, "res": res})



@app.route('/notify', methods = ['POST'])
def notify():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    read_set = data['read_set']
    write_set = data['write_set']
    txTable.notifyTX(transaction_id)
    logging.info(f"Validated: transaction_id: {transaction_id}, read_set: {read_set}, write_set: {write_set}")
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
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()