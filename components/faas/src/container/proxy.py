import os
import time
import boto3
from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
from Store import RedisShadowTable, RedisCache


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
        self.shadow_table = None
        self.cache = None
        self.ctx = {}

    def init(self, workflow, function, node_list):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function
        self.node_list = node_list
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB)
        # local cache
        self.cache = RedisCache(container_config.REDIS_PORT, container_config.REDIS_CACHE_DB, db_server)

        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')

        print('init finished...')

    def run(self, transaction_id, function_pos, input, output, write_set, is_repair, downstream_func_table):
        # FaaSStore
        
        TxMetaData_thisFunc = {"ReadSet": {}, "WriteSet": write_set, "DownstreamFuncTable":downstream_func_table, "RYW_subjection": {}}
        store = Store(self.workflow, self.function, transaction_id, input, output, function_pos, self.shadow_table, self.cache, TxMetaData_thisFunc, is_repair)
        self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}

        # pre-exec
        exec(self.code, self.ctx)

        input_dict = store.input

        # run function
        out = eval('main()', self.ctx)
      

        return TxMetaData_thisFunc["ReadSet"], TxMetaData_thisFunc["WriteSet"],TxMetaData_thisFunc["RYW_subjection"],store.io_latency


proxy = Flask(__name__)
proxy.status = 'new'
proxy.debug = False
runner = Runner()


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
    runner.init(inp['workflow'], inp['function'],inp['node_list'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    input = inp['input']
    output = inp['output']
    function_pos = inp['function_pos']
    write_set = inp['write_set'] 
    is_repair = inp['is_repair']
    downstream_func_table = inp['downstream_func_table']


    # record the execution time
    rs, ws, RYW_subjection,io_latency = runner.run(transaction_id, function_pos, input, output, write_set, is_repair, downstream_func_table)

    res = {
        "read_set": rs,
        "write_set": ws,
        "RYW_upstreams": RYW_subjection,
        "io_latency": io_latency
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
