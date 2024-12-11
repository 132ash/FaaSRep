from gevent import monkey
monkey.patch_all()
import os
import gevent
import json
import redis
from typing import Dict
import sys
sys.path.append('../../config')
import config
import workersp_repo
from workersp import WorkerSPManager
import docker
from flask import Flask, request
app = Flask(__name__)
docker_client = docker.from_env()
container_names = []

def clearRedis():
    repo = workersp_repo.Repository()
    repo.clear_mem()

class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       clearRedis()
       self.managers = {name: WorkerSPManager(sys.argv[1] + ':' + sys.argv[2], name, addr) for name, addr in info_addrs.items()}

    def get_state(self, workflow_name: str, transaction_id: str, function_pos={}, read_set={}, write_set={}) -> WorkerSPManager:
        return self.managers[workflow_name].get_state(transaction_id, function_pos, read_set, write_set)

    def trigger_function(self, workflow_name, state, function_name, no_parent_execution):
        self.managers[workflow_name].trigger_function(state, function_name, no_parent_execution)
    
    def clear_mem(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_mem(transaction_id)
    
    def clear_db(self, workflow_name, transaction_id):
        self.managers[workflow_name].clear_db(transaction_id)
    
    def del_state(self, workflow_name, transaction_id, master):
        self.managers[workflow_name].del_state(transaction_id, master)

dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)

# a new request from outside
# the previous function was done
@app.route('/request', methods = ['POST'])
def req():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    workflow_name = data['workflow_name']
    function_name = data['function_name']
    no_parent_execution = data['no_parent_execution']
    function_pos = data.get('function_pos', {})
    read_set = data.get('read_set', {})
    write_set = data.get('write_set', {})
    # get the corresponding workflow state and trigger the function
    state = dispatcher.get_state(workflow_name, transaction_id, function_pos, read_set, write_set)
    dispatcher.trigger_function(workflow_name, state, function_name, no_parent_execution)
    return json.dumps({'status': 'ok'})

@app.route('/clear', methods = ['POST'])
def clear():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    master = False
    if 'master' in data:
        master = True
        dispatcher.clear_db(workflow_name, transaction_id) # optional: clear results in center db
    dispatcher.clear_mem(workflow_name, transaction_id) # must clear memory after each run 
    dispatcher.del_state(workflow_name, transaction_id, master) # and remove state for every node
    return json.dumps({'status': 'ok'})

@app.route('/info', methods = ['GET'])
def info():
    return json.dumps(container_names)

@app.route('/clear_container', methods = ['GET'])
def clear_container():
    print('clearing containers')
    os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
    return json.dumps({'status': 'ok'})

GET_NODE_INFO_INTERVAL = 0.1

def get_container_names():
    gevent.spawn_later(get_container_names)
    global container_names
    container_names = [container.attrs['Name'] for container in docker_client.containers.list()]

    
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
    gevent.spawn_later(GET_NODE_INFO_INTERVAL)