import os
import time
import couchdb
from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
import redis
from Store import RedisShadowTable, RedisCache


couchdb_url = container_config.COUCHDB_URL
db_server = couchdb.Server(couchdb_url)

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

    def run(self, transaction_id, function_pos, input, output):
        # FaaSStore
        TxMetaData = {"ReadSet": {}, "WriteSet": set()}
        store = Store(self.workflow, self.function, transaction_id, input, output, function_pos, self.shadow_table, self.cache, TxMetaData)
        self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}

        # pre-exec
        exec(self.code, self.ctx)

        input_dict = store.input

        # run function
        start = time.time()
        out = eval('main()', self.ctx)
        end = time.time()

        return TxMetaData["ReadSet"], list(TxMetaData["WriteSet"])


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

    # record the execution time
    start = time.time()
    rs, ws = runner.run(transaction_id, function_pos, input, output)
    end = time.time()

    res = {
        "start_time": start,
        "end_time": end,
        "duration": end - start,
        "inp": inp, 
        "input": input,
        "output": output,
        "read_set": rs,
        "write_set": ws
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
