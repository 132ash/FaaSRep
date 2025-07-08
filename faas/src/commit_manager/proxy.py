from gevent import monkey
monkey.patch_all()
from flask import Flask, request
import sys
from validator import ValidatorPool
from FaaSTCC_storage import FaaSTCC_StorageLayer
import json
from datetime import datetime
app = Flask(__name__)

sys.path.append('../../config')
import config

GATEWAY_ADDR = config.GATEWAY_ADDR
VALIDATE = 1
COMMIT = 2
PESSIMISTIC_REPAIR_FINISH = 4

workflows = config.FUNCTION_INFO_ADDRS.keys()
validator_pools = {workflow: ValidatorPool(config.VALIDATORS_PER_POOL) for workflow in workflows} if config.REPAIR else None


FaaSTCC_storage_layers = {workflow: FaaSTCC_StorageLayer(workflow) for workflow in workflows} if config.FAASTCC else None


# receive a set of rw sets and validate them. they belongs to the same workflow.
# read set: {func: {key: version}}  write set: {key: {ip:func_ip, func:func}}
@app.route('/validate', methods=['POST'])
def validate_tx():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow_name']
    batch_id = data['batch_id']
    validator_pools[workflow].submit(batch_id, VALIDATE, data)
    return json.dumps({'status': 'processing'})  

@app.route('/pessi_fin', methods=['POST'])
def pessi_finish():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow_name']
    batch_id = data['batch_id']
    validator_pools[workflow].submit(batch_id, PESSIMISTIC_REPAIR_FINISH, data)
    return json.dumps({'status': 'successed'})

@app.route('/FaaSTCC_get', methods=['POST'])
def faastcc_get():
    data = request.get_json(force=True, silent=True)
    key  = data['key']
    version_target = data['version']
    workflow = data['workflow_name']
    nearest_version, promise = FaaSTCC_storage_layers[workflow].FaaSTCC_get(version_target, key)
    return json.dumps({'nearest_version': nearest_version, 'promise': promise})
    
@app.route('/commit', methods = ['POST'])
def transaction_commit():
    data = request.get_json(force=True, silent=True)
    if config.REPAIR:
        workflow = data['workflow_name']
        batch_id = data['batch_id']
        validator_pools[workflow].submit(batch_id, COMMIT, {})
    else:
        version = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        FaaSTCC_storage_layers[workflow].FaaSTCC_commit(data['transaction_id'], data['write_set'], version)
    return json.dumps({'status': 'successed'})

# python3 proxy.py 192.168.162.132 9000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()