import os
import time
import couchdb
from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
import redis

default_file = 'main.py'
work_dir = '/proxy'
redis_server = redis.StrictRedis(host=container_config.REDIS_HOST, port=container_config.REDIS_PORT, db=container_config.REDIS_DB)

class Runner:
    def __init__(self):
        self.code = None
        self.workflow = None
        self.function = None
        self.ctx = {}

    def init(self, workflow, function):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function

        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')

        print('init finished...')

    def run(self, transaction_id, input, output):
        # FaaSStore
        store = Store(self.workflow, self.function, transaction_id, input, output, redis_server)
        self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}

        # pre-exec
        exec(self.code, self.ctx)

        input_dict = store.input

        # run function
        start = time.time()
        out = eval('main()', self.ctx)
        end = time.time()


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
    runner.init(inp['workflow'], inp['function'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    input = inp['input']
    output = inp['output']

    # record the execution time
    start = time.time()
    runner.run(transaction_id, input, output)
    end = time.time()

    res = {
        "start_time": start,
        "end_time": end,
        "duration": end - start,
        "inp": inp, 
        "input": input,
        "output": output,
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
