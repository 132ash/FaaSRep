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

VALIDATE_INTERVAL = config.VALIDATE_INTERVAL


class Dispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
       self.host_addr = sys.argv[1] + ':' + sys.argv[2]
       self.sinks = {name: TransactionSink(name, config.BATCH_SIZE, self.host_addr) for name in info_addrs}  
       gevent.spawn_later(VALIDATE_INTERVAL, self._validate_loop)

    def validate_transaction(self, workflow_name, transaction_id, read_set, write_set):
        self.sinks[workflow_name].append(transaction_id, read_set, write_set)


    def _validate_loop(self):
        gevent.spawn_later(VALIDATE_INTERVAL, self._validate_loop)
        for sink in self.sinks.values():
            gevent.spawn(sink.validate_batch_check)

dispatcher = Dispatcher(info_addrs=config.WORKFLOW_YAML_ADDR)


@app.route('/validate', methods = ['POST'])
def validate():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    read_set = data['read_set']
    write_set = data['write_set']
    dispatcher.validate_transaction(workflow_name, transaction_id, read_set, write_set)
    return json.dumps({'status': 'ok'})

# python3 proxy.py  10.2.30.50 6000
# python3 proxy.py  10.2.27.23 6000
# python3 proxy.py  10.2.30.62 6000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   