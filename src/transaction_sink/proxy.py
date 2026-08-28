import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / 'config'))
from experiment_logging import configure_root_experiment_logging
configure_root_experiment_logging('transaction_sink_runtime')


from gevent import monkey
monkey.patch_all()
import os
import gevent
import time
import requests
import json
from typing import Dict
from datetime import datetime
import docker
from flask import Flask, request
app = Flask(__name__)
docker_client = docker.from_env()
container_names = []
from validate_struct import TransactionSink

sys.path.append('../../config')
import config

VALIDATE_INTERVAL = config.VALIDATE_INTERVAL

FAST_PATH = config.FAST_PATH
PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR

OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

REPAIRED = config.REPAIRED
ABORTED = config.ABORTED
WAITING = config.RUNNING


class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.sinks = {name: TransactionSink(name, config.BATCH_SIZE, self.host_addr) for name in info_addrs}
       gevent.spawn_later(VALIDATE_INTERVAL, self._validate_loop)

    def fin_repair_or_abort_within_batch(self, workflow_name, batch_id, transaction_id,repair_mode, state, skip_repair=False, repair_epoch=1, attempt_id='', error=''):
        self.sinks[workflow_name].fin_repair_or_abort(batch_id, transaction_id, repair_mode, state, skip_repair, repair_epoch, attempt_id, error)

    def register_repair_info_after_validate(self, workflow_name, batch_id,
                                            batch_sub, tx_sub, sub_per_tx,
                                            transaction_list):
        return self.sinks[workflow_name].register_repair_info_after_validate(
            batch_id, batch_sub, tx_sub, sub_per_tx, transaction_list)

    def validate_transaction(self, workflow_name, transaction_id, read_set, write_set, container_port, RYW_subjection, transaction_metadata=None):
        self.sinks[workflow_name].append(transaction_id, read_set, write_set, container_port, RYW_subjection, transaction_metadata)

    def sink_release_optimistic_info(self, workflow_name, batch_list):
        self.sinks[workflow_name].clear_opt_table_after_finish(batch_list)

    def _validate_loop(self):
        gevent.spawn_later(VALIDATE_INTERVAL, self._validate_loop)
        for sink in self.sinks.values():
            gevent.spawn(sink.validate_batch_check)



dispatcher = Dispatcher(info_addrs=config.WORKFLOW_YAML_ADDR)

@app.route('/fin_repair', methods = ['POST'])
def fin_repair():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    repair_mode = data['repair_mode']
    skip_repair = data.get('skip_repair', False)
    # logging.info(f"[FIN REPAIR] workflow: {workflow_name}, batch_id: {batch_id}, transaction_id: {transaction_id}, repair_mode: {repair_mode}")
    dispatcher.fin_repair_or_abort_within_batch(workflow_name, batch_id, transaction_id, repair_mode, REPAIRED, skip_repair,
                                                data.get('repair_epoch', 1), data.get('attempt_id', ''))
    return json.dumps({'status': 'ok'})

@app.route('/abort', methods = ['POST'])
def abort():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    error = data.get("error", "")
    logging.info(f"[ABORT] workflow: {workflow_name}, transaction_id: {transaction_id}, REPAIR: {data.get('repair', False)}, error:{error}")
    if data.get('repair', False):
        dispatcher.fin_repair_or_abort_within_batch(workflow_name, data['batch_id'], transaction_id,  data['repair_mode'], ABORTED,
                                                    repair_epoch=data.get('repair_epoch', 1), attempt_id=data.get('attempt_id', ''), error=error)
    else:
        notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
        payload = {
            'transaction_id_lists': [[transaction_id]],
            'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
            'abort': True,
            'pessimistic_txs':[{}]
        }
        requests.post(notify_url, json=payload)
    return json.dumps({'status': 'ok'})

@app.route('/validate', methods = ['POST'])
def validate():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    read_set = data['read_set']
    write_set = data['write_set']
    container_port = data['container_port']
    RYW_subjection = data.get('RYW_subjection', {})
    transaction_metadata = data.get('transaction_metadata', {})
    dispatcher.validate_transaction(workflow_name, transaction_id, read_set, write_set, container_port, RYW_subjection, transaction_metadata)
    return json.dumps({'status': 'ok'})

@app.route('/repair_pessi', methods = ['POST'])
def repair_pessimistic():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    batch_id = data['batch_id']
    batch_sub =  data['batch_sub']
    tx_sub =  data['tx_sub']
    sub_per_tx = data.get('whole_tx_sub', {})
    transaction_list = data.get('transaction_list', [])
    res = dispatcher.register_repair_info_after_validate(
        workflow_name, batch_id, batch_sub, tx_sub, sub_per_tx,
        transaction_list)
    return res

@app.route('/release_opt', methods = ['POST'])
def release_opt_table():
    data = request.get_json(force=True, silent=True)
    batch_list = data['batch_list']
    workflow_name = data['workflow_name']
    dispatcher.sink_release_optimistic_info(workflow_name, batch_list)
    return json.dumps({'status': 'ok'})

# python3 proxy.py  10.2.30.50 6000
# python3 proxy.py  10.2.27.23 6000
# python3 proxy.py  10.2.30.62 6000
# python3 proxy.py  10.2.29.142  6000

from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app,
                        log=None, error_log=None)
    server.serve_forever()
