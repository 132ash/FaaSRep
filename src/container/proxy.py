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
from redis_component import RedisShadowTable, RedisCache

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,  # 设置日志级别为 INFO
    format='%(asctime)s [%(levelname)s] %(message)s',  # 日志格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ],
    force=True
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
        self.workflow = None
        self.function = None
        self.node_list = None
        self.cache = None
        self.function_pos = None
        self.ctx = {}

        # infomation saved in first run
        self.transaction_id = None
        self.write_set = {}

    def init(self, host_addr, workflow, function, node_list,input,output,function_pos, cache_enable):
        # update function status
        self.host_addr = host_addr
        self.workflow = workflow
        self.function = function
        self.input = input
        self.cache_enable = cache_enable
        self.output = output
        self.function_pos = function_pos
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB, self.host_addr, db_server, cache_enable)
        # local cache
        self.cache = RedisCache(container_config.REDIS_CACHE_PORT, container_config.REDIS_CACHE_DB, db_server, cache_enable)
        os.chdir(work_dir)
        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.function, self.shadow_table, self.cache, db_server, function_pos, self.input, self.output)

    def save(self, transaction_id, write_set):  
        self.transaction_id = transaction_id
        self.write_set = write_set

    def run(self, transaction_id):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": self.write_set, 
                                'cache_enable':self.cache_enable
                              }
        aborted = False
        msg = ''
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        #print(f"Running function: {self.function}, transaction_id: {transaction_id}, is_repair: {is_repair}, dirty: {self.dirty}, fast_path_enabled: {self.fast_path_enabled},write_set: {self.write_set}, parent_cnt: {self.parent_cnt}, repair_metadata:{TxMetaData_thisFunc}", flush=True)
        # need run: first run / repair, in fast-path and dirty / repair, not in fast-path.
        store.runtime_init(transaction_id, TxMetaData_thisFunc)
        self.ctx = {'workflow_name': self.workflow, 'function_name': self.function, 'store': store}
        # pre-exec
        try:
            exec(self.code, self.ctx)
            out = eval('main()', self.ctx)               
        except Exception as e:
            aborted = True
            msg = json.dumps({'Abort': True, 'error': str(e), 'io_latency':store.io_latency})
            logging.error(f"Function {self.function} execution failed: {msg}")
        # the function finished repair, not abort, send data to waiting functions in fastpath..
        io_latency = store.io_latency

        return aborted, msg, TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"], io_latency


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
    runner.init(inp['host_addr'], inp['workflow'], inp['function'], 
                inp['node_list'], inp['input'],inp['output'],inp['function_pos'],inp.get('cache_enable', False))

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'
    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    io_latency = 0
    rs, ws={},{}
    # first run, or not the reserved container. Save the info for this container.
    # set the state to running.
    runner.save(transaction_id, inp['write_set'])
        
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    aborted, abort_msg, rs, ws, io_latency = runner.run(transaction_id)
    if aborted:
        return abort_msg

    res = {
        "read_set": rs,
        "write_set": ws,
        "io_latency": io_latency
    }

    proxy.status = 'ok'
    return res

if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
