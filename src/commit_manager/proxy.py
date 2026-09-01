from gevent import monkey
monkey.patch_all()
from flask import Flask, jsonify, request
import sys
import json
from pathlib import Path

from lock_manager import LockManager

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import config

app = Flask(__name__)
manager = LockManager(getattr(config, 'LOCK_WAIT_DEADLINE_SECONDS', 30.0))

# Preserve the existing OCC service contract when the process is started in
# its default mode.  Boki-SN does not construct validator pools at all.
validator_pools = None
if config.SYSTEM_MODE != 'BOKI_SN':
    from validator import ValidatorPool
    validator_pools = {workflow: ValidatorPool(config.VALIDATORS_PER_POOL, workflow)
                       for workflow in config.WORKFLOW_YAML_ADDR}


def body():
    return request.get_json(force=True, silent=False)


@app.route('/validate', methods=['POST'])
def validate_tx():
    if validator_pools is None:
        return jsonify({'status': 'unsupported', 'error': 'validator is disabled in BOKI_SN'}), 409
    data = body()
    validator_pools[data['workflow_name']].submit(data['batch_id'], 1, data)
    return json.dumps({'status': 'processing'})


@app.route('/begin', methods=['POST'])
def begin():
    data = body()
    return jsonify(manager.begin(data['txid'], int(data.get('term', 0)), data.get('global_req_id')))


@app.route('/lock', methods=['POST'])
def lock():
    data = body()
    result = manager.lock(data['txid'], int(data['term']), int(data['birth_seq']), data['key'],
                          data['mode'], data['op_id'], data.get('deadline_seconds'))
    return jsonify(result)


@app.route('/unlock', methods=['POST'])
def unlock():
    data = body()
    return jsonify(manager.unlock(data['txid'], int(data['term']), bool(data.get('all', False))))


@app.route('/abort', methods=['POST'])
def abort():
    data = body()
    return jsonify(manager.abort(data['txid'], int(data['term']), data.get('abort_type', 'ERROR')))


@app.route('/debug/tx/<txid>', methods=['GET'])
def debug(txid):
    return jsonify(manager.debug_tx(txid))


@app.route('/health', methods=['GET'])
def health():
    return jsonify(manager.health())

# python3 proxy.py 10.2.29.142 9000


if __name__ == '__main__':
    from gevent.pywsgi import WSGIServer
    import logging
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
