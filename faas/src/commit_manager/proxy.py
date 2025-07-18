from gevent import monkey
monkey.patch_all()
from flask import Flask, request
import sys
from FaaSTCC_storage import FaaSTCC_StorageLayer
import json
from datetime import datetime
app = Flask(__name__)

sys.path.append('../../config')
import config

workflows = config.FUNCTION_INFO_ADDRS.keys()
FaaSTCC_storage_layers = {workflow: FaaSTCC_StorageLayer(workflow) for workflow in workflows}


@app.route('/FaaSTCC_get', methods=['POST'])
def faastcc_get():
    data = request.get_json(force=True, silent=True)
    key  = data['key']
    version_target = data['version']
    workflow = data['workflow_name']
    nearest_version, promise = FaaSTCC_storage_layers[workflow].FaaSTCC_get(version_target, key)
    print(f"FaaSTCC_get: {key} with target version {version_target}, nearest version is {nearest_version}, promise is {promise}")
    return json.dumps({'version': nearest_version, 'promise': promise})
    
@app.route('/commit', methods = ['POST'])
def transaction_commit():
    data = request.get_json(force=True, silent=True)
    version = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    first_run_finish_time = data['first_run_finish_time']
    FaaSTCC_storage_layers[data['workflow_name']].FaaSTCC_commit(data['transaction_id'], data['write_set'], version, first_run_finish_time)
    return json.dumps({'status': 'successed'})


# python3 proxy.py 192.168.162.132 9000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()