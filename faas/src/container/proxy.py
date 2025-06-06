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

        # infomation saved in first run
        self.transaction_id = None
        self.input = {}
        self.output = {}
        self.function_pos_inside_tx = {}
        self.write_set = {}
        self.is_repair = None
        self.next_functions = None
        self.parent_cnt = None

        self.parent_executed = 0

        # infomation fetched from Redis in repair
        self.repair_metadata_lock = gevent.lock.BoundedSemaphore()
        self.repair_metadata_fetched = False
        self.dirty = False
        self.function_pos_whole_batch = {}
        self.upstream_func_count = 0
        self.upstream_fetched = 0
        self.keys_from_upstream = {}

        # fast path enabled
        self.fast_path_enabled = False
        # remote lock enabled
        self.remote_lock_enabled = False
        self.lock_set = {}


    def init(self, workflow, function, node_list, fast_path_enabled, remote_lock_enabled):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function
        self.node_list = node_list
        self.fast_path_enabled = fast_path_enabled
        self.remote_lock_enabled = remote_lock_enabled
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, function, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB)
        # local cache
        self.cache = RedisCache(container_config.REDIS_PORT, container_config.REDIS_CACHE_DB, db_server)

        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')

        print('init finished...')

    def save(self, transaction_id, input, output, function_pos, write_set, next_functions, parent_cnt, lock_set):
        self.transaction_id = transaction_id
        self.input = input
        self.output = output
        self.function_pos_inside_tx = function_pos
        self.write_set = write_set
        self.next_functions = next_functions
        self.parent_cnt = parent_cnt
        self.lock_set = lock_set

    def fetch_repair_metadata(self, batch_id, transaction_id):
        self.repair_metadata_lock.acquire()
        if not self.repair_metadata_fetched:
            self.repair_metadata_fetched = True
            redis_key = f"{transaction_id}:REPAIR:{self.function}:" 
            self_ip = self.function_pos_inside_tx[self.function]['ip']
            try:
                metadata_string = self.shadow_table.raw_fetch_data(redis_key, self_ip)
            except KeyError:
                metadata_string = None
            if metadata_string:
                repair_metadata = json.loads(metadata_string)
                self.upstream_func_count = repair_metadata['key_subjection']['up_cnt']
                self.keys_from_upstream = repair_metadata['key_subjection']['upstream_keys']
                self.dirty = repair_metadata['dirty']
            self.function_pos_whole_batch = json.loads(self.shadow_table.raw_fetch_data(f"{batch_id}:POS::", self_ip))
            self.parent_cnt += self.upstream_func_count
            logging.info(f"Fetched repair metadata:{self.function_pos_whole_batch}, upstream_func_count: {self.upstream_func_count}, keys_from_upstream: {self.keys_from_upstream}, dirty: {self.dirty}")
            self.repair_metadata_lock.release()

    def check_runnable(self, is_repair, no_parent_execution):
        # not in repair mode, check is finished outside the container.
        if not is_repair or no_parent_execution or not self.fast_path_enabled:
            return True
        else:
            self.parent_executed += 1
            logging.info(f"Parent executed: {self.parent_executed}, parent_cnt: {self.parent_cnt}")
            return self.parent_executed == self.parent_cnt
        
    def trigger_next_function(self, batch_id, transaction_id, ip, port, dirty):
        url = f'http://{ip}:{port}/run'
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'repair': True,
            'dirty':dirty
            }
        logging.info(f"Triggering next function: {ip}:{port}, batch_id: {batch_id}, transaction_id: {transaction_id}, dirty: {dirty}")
        requests.post(url, json=data)

    def fin_repair(self, batch_id, transaction_id, ip):
        logging.info(f"Finishing repair: {self.function}")
        url = f'http://{ip}:7000/fin_repair'
        data = {'batch_id': batch_id, "transaction_id": transaction_id}
        requests.post(url, json=data)

    def run(self, batch_id, transaction_id, is_repair):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": self.write_set, 
                                "lock_set": self.lock_set,
                                "RYW_subjection": {},
                                "function_pos_whole_batch":self.function_pos_whole_batch,
                                "dirty": self.dirty,
                               }
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        logging.info(f"Running function: {self.function}, transaction_id: {transaction_id}, is_repair: {is_repair}, dirty: {self.dirty}, fast_path_enabled: {self.fast_path_enabled}, input: {self.input}, output: {self.output}, function_pos_inside_tx: {self.function_pos_inside_tx}, write_set: {self.write_set}, RYW_upstream:{self.RYW_upstream}, next_functions: {self.next_functions}, parent_cnt: {self.parent_cnt}, lock_set:{self.lock_set}")
        if not self.fast_path_enabled or not is_repair or self.dirty:
            store = Store(self.function, transaction_id, self.input, self.output, self.function_pos_inside_tx, self.shadow_table, self.cache, TxMetaData_thisFunc, is_repair, self.fast_path_enabled, self.remote_lock_enabled, db_server)
            self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}

            # pre-exec
            exec(self.code, self.ctx)
            # run function
            out = eval('main()', self.ctx)
        # in repair mode and in fast-path: trigger next function inside the container.
        logging.info(f"Trigger Next functions: {self.next_functions}")
        if self.fast_path_enabled and is_repair:
            next_trigger_tasks = []
            for next_func in self.next_functions:
                if next_func == 'END':
                    self_ip = self.function_pos_inside_tx[self.function]['ip']
                    next_trigger_tasks.append(gevent.spawn(self.fin_repair, batch_id, self.transaction_id, self_ip))
                    break
                ip = self.function_pos_whole_batch[self.transaction_id][next_func]['ip']
                port = self.function_pos_whole_batch[self.transaction_id][next_func]['port']
                logging.info(f"Next function: {next_func}, batch_id: {batch_id}, ip: {ip}, port: {port}")
                next_trigger_tasks.append(gevent.spawn(self.trigger_next_function, batch_id, self.transaction_id, ip, port, self.dirty))
            gevent.joinall(next_trigger_tasks)

        io_latency = 0
        lock_latency = 0
        if self.remote_lock_enabled:
            lock_latency = store.lock_latency

        if not self.fast_path_enabled or not is_repair or self.dirty:
            io_latency = store.io_latency

        return TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency, lock_latency


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
    runner.init(inp['workflow'], inp['function'],inp['node_list'], inp['fast_path_enabled'], inp['remote_lock_enabled'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    io_latency, lock_latency = 0, 0
    is_repair = inp.get('repair',False)
    no_parent_execution = False
    batch_id = ""
    lock_set = {}
    rs, ws, RYW_subjection={},{},{}
    # first run, or not the reserved container. Save the info for this container.
    if not is_repair or not runner.fast_path_enabled:
        input = inp['input']
        lock_set = inp['lock_set']
        output = inp['output']
        function_pos = inp['function_pos']
        write_set = inp['write_set'] 
        next_functions = inp['next_functions']
        parent_cnt = inp['parent_cnt']
        runner.save(transaction_id, input, output, function_pos, write_set, next_functions, parent_cnt, lock_set)
    else:
        batch_id = inp['batch_id']
        if runner.fast_path_enabled:
            no_parent_execution = inp.get('no_parent_execution', False)
            # get the info from redis
            runner.fetch_repair_metadata(batch_id, transaction_id)
        
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    if runner.check_runnable(is_repair, no_parent_execution):
        if runner.remote_lock_enabled:
            try:
                rs, ws, RYW_subjection, io_latency, lock_latency = runner.run(batch_id, transaction_id, is_repair)
            except Exception as e:
                return json.dumps({'Error':True, 'error': str(e), 'lock_set':runner.lock_set})
        else:
            rs, ws, RYW_subjection, io_latency, lock_latency = runner.run(batch_id, transaction_id, is_repair)

    res = {
        "read_set": rs,
        "write_set": ws,
        "RYW_upstreams": RYW_subjection,
        "io_latency": io_latency,
        "lock_set": lock_set,
        "lock_latency": lock_latency
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
