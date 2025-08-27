from gevent import monkey
monkey.patch_all()
from pathlib import Path
import time
import sys
import json
import gevent.event as event
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

VALIDATE_URL =  f'http://{VALIDATOR_ADDR}/validate'
workflow_name = 'c4'

class RunningTXTable:
    def __init__(self):
        self.infos = {}
    
    def registerTX(self, batch_id):
        self.infos[batch_id] = {'cond':event.Event(), 'finished':False}

    def waitTX(self, batch_id):
        info = self.infos[batch_id]
        cond = info['cond']
        cond.clear()
        while not info['finished']:
            cond.wait()
        return True

    def finishTX(self, batch_id):
        info = self.infos[batch_id]
        info['finished'] = True
        info['cond'].set()

txTable = RunningTXTable()

@app.route('/fake_request', methods = ['POST'])
def request_handler():
    data = request.get_json(force=True, silent=True)
    fake_batch = data['fake_batch']
    txTable.registerTX(fake_batch['batch_id'])
    fake_batch['first_run_finish_time'] = time.time()
    requests.post(VALIDATE_URL, json=fake_batch)
    txTable.waitTX(fake_batch['batch_id'])
    return json.dumps({'status': 'ok'})

@app.route('/fake_notify', methods = ['POST'])
def repair_pessimistic():
    data = request.get_json(force=True, silent=True)
    batch_id_list = data['batch_id_list']
    for batch_id in batch_id_list:
        txTable.finishTX(batch_id)
        logging.info(f"[FAKE NOTIFY] Finished processing for batch {batch_id}")
    return json.dumps({'status': 'ok'})

# python proxy.py  10.2.29.142 8000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()

