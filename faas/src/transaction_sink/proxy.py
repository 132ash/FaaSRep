import sys
import logging
# 配置日志记录
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    # 设置日志级别为 INFO
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S',  # 设置日期格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ],
    force=True 
)


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

validate_interval = 0.005 # 200 qps at most

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
       gevent.spawn_later(validate_interval, self._validate_loop)

    def fin_repair_or_abort_within_batch(self, workflow_name, batch_id, transaction_id,repair_mode, state):
        self.sinks[workflow_name].fin_repair_or_abort(batch_id, transaction_id, repair_mode, state)

    def register_repair_info_after_validate(self, workflow_name, batch_id, batch_sub, tx_sub, sub_per_tx):
        return self.sinks[workflow_name].register_repair_info_after_validate(batch_id, batch_sub, tx_sub, sub_per_tx)
    
    def validate_transaction(self, workflow_name, transaction_id, read_set, write_set, container_port, RYW_subjection):
        self.sinks[workflow_name].append(transaction_id, read_set, write_set, container_port, RYW_subjection)

    def _validate_loop(self):
        gevent.spawn_later(validate_interval, self._validate_loop)
        for sink in self.sinks.values():
            gevent.spawn(sink.validate_batch)



dispatcher = Dispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)

@app.route('/fin_repair', methods = ['POST'])
def fin_repair():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    repair_mode = data['repair_mode']
    dispatcher.fin_repair_or_abort_within_batch(workflow_name, batch_id, transaction_id, repair_mode, REPAIRED)
    return json.dumps({'status': 'ok'})

@app.route('/abort', methods = ['POST'])
def abort():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    if data.get('repair', False):
        dispatcher.fin_repair_or_abort_within_batch(workflow_name, data['batch_id'], transaction_id,  data['repair_mode'], ABORTED)
    # delay.
    else:
        notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
        payload = {
            'transaction_id_lists': [[transaction_id]],
            'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
            'abort': True
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
    dispatcher.validate_transaction(workflow_name, transaction_id, read_set, write_set, container_port, RYW_subjection)
    return json.dumps({'status': 'ok'})

@app.route('/repair_pessi', methods = ['POST'])
def repair_pessimistic():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    batch_id = data['batch_id']
    batch_sub =  data['batch_sub']
    tx_sub =  data['tx_sub']  
    sub_per_tx = data.get('whole_tx_sub', {})
    res = dispatcher.register_repair_info_after_validate(workflow_name, batch_id, batch_sub, tx_sub, sub_per_tx)
    logging.info(f"Registered pessimistic repair info for batch_id {batch_id}, return: {res}")
    return res

# python3 proxy.py  10.2.30.52 6000
# python3 proxy.py  10.2.27.24 6000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   