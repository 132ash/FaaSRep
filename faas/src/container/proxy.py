from gevent import monkey
monkey.patch_all()
import requests
import os
import gevent
import gevent.lock
import logging
import json
import sys
import boto3

from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
from redis_component import RedisShadowTable, RedisCache

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别为 INFO
    format='%(asctime)s [%(levelname)s] %(message)s',  # 日志格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ]
)


dynamodb_url = container_config.DYNAMODB_URL
dynamodb_key_id = container_config.DYNAMODB_KEY_ID
dynamodb_access_key = container_config.DYNAMODB_ACCESS_KEY
dynamodb_area = container_config.DYNAMODB_AREA
RUNNING = container_config.RUNNING
ABORTED = container_config.ABORTED
REPAIRED = container_config.REPAIRED
db_server = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)

default_file = 'main.py'
work_dir = '/proxy'

class Runner:
    def __init__(self):
        self.code = None
        self.workflow = None
        self.function = None
        self.shadow_table = None
        self.cache = None
        self.ctx = {}
        self.transaction_id = None
        self.input = {}
        self.output = {}
        self.write_set = {}

    def init(self, workflow, function,input,output,function_pos, port, node_list, validator_addr):
        # update function status
        self.workflow = workflow
        self.function = function
        self.input = input
        self.output = output
        self.port = port
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB)
        # local cache
        self.cache = RedisCache(container_config.REDIS_PORT, container_config.REDIS_CACHE_DB, db_server)
        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.workflow, self.function, self.shadow_table, self.cache, db_server, function_pos, validator_addr)

        logging.info('init finished...')

    def save(self, transaction_id, write_set, snapshot_interval):
        self.transaction_id = transaction_id
        self.write_set = write_set
        self.snapshot_interval = snapshot_interval

    def run(self, transaction_id):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": self.write_set, 
                                'snapshot_interval': self.snapshot_interval
                              }
        aborted = False
        msg = ''
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        logging.info(f"Running function: {self.function}, transaction_id: {transaction_id}, input: {self.input}, output: {self.output}, write_set: {self.write_set}")
        # need run: first run / repair, in fast-path and dirty / repair, not in fast-path.
        store.runtime_init(self.input, self.output, transaction_id, TxMetaData_thisFunc)
        self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}
        try:
            exec(self.code, self.ctx)
        # run function
            out = eval('main()', self.ctx)               
        except Exception as e:
            aborted = True
            msg = json.dumps({'Abort': True, 'error': str(e)})
        io_latency = store.io_latency
        return aborted, msg, TxMetaData_thisFunc["write_set"], TxMetaData_thisFunc['snapshot_interval'], io_latency


proxy = Flask(__name__)
proxy.status = 'new'
proxy.debug = False
runner = Runner()
store = Store()


@proxy.route('/status', methods=['GET'])
def status():
    res = {}
    res['status'] = proxy.status
    res['workdir'] = os.getcwd()
    if runner.function:
        res['function'] = runner.function
    return res


@proxy.route('/init', methods=['POST'])
def init():
    proxy.status = 'init'

    inp = request.get_json(force=True, silent=True)
    runner.init(inp['workflow'], inp['function'],   
                inp['input'],inp['output'],
                inp['function_pos'], inp['port'], inp['node_list'], inp['validator_addr'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    io_latency = 0
    snapshot_interval = inp['snapshot_interval']
    runner.save(transaction_id, inp['write_set'], snapshot_interval)
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    aborted, abort_msg, ws, snapshot_interval, io_latency = runner.run(transaction_id)
    if aborted:
        return abort_msg

    res = {
        "write_set": ws,
        "io_latency": io_latency,
        'snapshot_interval': snapshot_interval,
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
