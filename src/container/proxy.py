from gevent import monkey
monkey.patch_all()
import os
import logging
import json
import sys
import boto3

from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config

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
db_server = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)

default_file = 'main.py'
work_dir = '/proxy'

class Runner:
    def __init__(self):
        self.code = None
        self.function = None
        self.ctx = {}

        # infomation saved in first run
        self.transaction_id = None
        self.input = {}
        self.output = {}
        self.write_set = {}
        self.lock_set = {}

    def init(self, function,input,output, port):
        # update function status

        self.function = function
        self.input = input
        self.output = output
        self.port = port
        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.function,  db_server)

        #logging.info('init finished...')

    def save(self, transaction_id, write_set, lock_set, create_timestamp):
        self.transaction_id = transaction_id
        self.write_set = write_set
        self.lock_set = lock_set
        self.create_timestamp = create_timestamp

    def run(self, transaction_id):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "write_set": self.write_set, 
                                "lock_set": self.lock_set,
                                'create_timestamp': self.create_timestamp,
                              }
        aborted = False
        msg = ''
        
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        #logging.info(f"Running function: {self.function}, transaction_id: {transaction_id}, input: {self.input}, output: {self.output}, write_set: {self.write_set}, lock_set:{self.lock_set}")
        # need run: first run / repair, in fast-path and dirty / repair, not in fast-path.
        store.runtime_init(self.input, self.output, transaction_id, TxMetaData_thisFunc)
        self.ctx = {'function_name': self.function, 'store': store}
        # pre-exec
        try:
            exec(self.code, self.ctx)
            # run function
            out = eval('main()', self.ctx)               
        except Exception as e:
            aborted = True
            msg = json.dumps({'Abort': True, 'error': str(e), 'lock_set': self.lock_set})       

        io_latency = store.io_latency
        lock_latency = store.lock_latency

        return aborted, msg, TxMetaData_thisFunc["write_set"], io_latency, lock_latency


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
    runner.init(inp['function'], inp['input'],inp['output'],inp['port'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'
    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    create_timestamp = inp['create_timestamp']
    lock_set = inp['lock_set']
    runner.save(transaction_id, inp['write_set'],  lock_set, create_timestamp)
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    aborted, abort_msg, ws, io_latency, lock_latency = runner.run(transaction_id)
    if aborted:
        return abort_msg

    res = {
        "write_set": ws,
        "io_latency": io_latency,
        "lock_set": lock_set,
        "lock_latency": lock_latency,
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
