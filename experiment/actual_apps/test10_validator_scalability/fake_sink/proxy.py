from gevent import monkey
monkey.patch_all()
from pathlib import Path
import json
import sys
import requests
from flask import Flask, request
app = Flask(__name__)


script_dir = Path(__file__).parent
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

from config.config import VALIDATOR_ADDR

COMMIT_URL =  f'http://{VALIDATOR_ADDR}/fin_repair'
workflow_name = 'c4'

@app.route('/fake_repair_pessi', methods = ['POST'])
def repair_pessimistic():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    ret_data = {batch_id: {'batch_finished': True, 'pessi_repair_txs': [], 'aborted_txs': []}}
    requests.post(COMMIT_URL, json={'workflow_name': workflow_name, 'data': ret_data})
    return json.dumps({'status': 'ok'})

# python proxy.py  10.2.29.142 6000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   