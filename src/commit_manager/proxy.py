from gevent import monkey
monkey.patch_all()
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / 'config'))
from experiment_logging import configure_root_experiment_logging
configure_root_experiment_logging('commit_manager_proxy')

from flask import Flask, request
from validator import ValidatorPool
app = Flask(__name__)

sys.path.append('../../config')
import config

GATEWAY_ADDR = config.GATEWAY_ADDR
VALIDATE = 1
REPAIR_FINISH = 2
CASCADED_COMMIT = 3

workflows = config.WORKFLOW_YAML_ADDR.keys()
validator_pools = {workflow: ValidatorPool(config.VALIDATORS_PER_POOL, workflow) for workflow in workflows}

# receive a set of rw sets and validate them. they belongs to the same workflow.
# read set: {func: {key: version}}  write set: {key: {ip:func_ip, func:func}}
@app.route('/validate', methods=['POST'])
def validate_tx():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow_name']
    batch_id = data['batch_id']
    # logging.info(f"[VALIDATE] Received validation request for batch {batch_id} in workflow {workflow}")
    validator_pools[workflow].submit(batch_id, VALIDATE, data)
    return json.dumps({'status': 'processing'})  

@app.route('/fin_repair', methods=['POST'])
def finish_repair(): 
    inp = request.get_json(force=True, silent=True)
    workflow_name = inp['workflow_name']
    data = inp['data']  
    # logging.info(f"[FIN REPAIR] Received repair finish request for workflow {workflow_name} with data: {data}")
    for batch_id, batch_data in data.items():
        validator_pools[workflow_name].submit(batch_id, REPAIR_FINISH, batch_data)
    return json.dumps({'status': 'successed'})

# python proxy.py  10.2.29.142  9000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app,
                        log=None, error_log=None)
    server.serve_forever()
