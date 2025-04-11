from gevent import monkey
monkey.patch_all()
import requests
import os
import gevent
import logging
import json
import sys
import boto3

from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
from Store import RedisShadowTable, RedisCache

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
        self.repair_metadata_fetched = False
        self.RYW_table = {}
        self.downstream_func_table = {}
        self.upstream_func_table = {}
        self.dirty = False
        self.function_pos_whole_batch = {}

        # fast path enabled
        self.fast_path_enabled = False


    def init(self, workflow, function, node_list, fast_path_enabled):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function
        self.node_list = node_list
        self.fast_path_enabled = fast_path_enabled
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

    def save(self, transaction_id, input, output, function_pos, write_set, next_functions, parent_cnt):
        self.transaction_id = transaction_id
        self.input = input
        self.output = output
        self.function_pos_inside_tx = function_pos
        self.write_set = write_set
        self.next_functions = next_functions
        self.parent_cnt = parent_cnt

    def fetch_repair_metadata(self, batch_id, transaction_id, dirty):
        self.dirty = dirty
        if not self.repair_metadata_fetched:
            redis_key = f"{transaction_id}:REPAIR:{self.function}:" 
            self_ip = self.function_pos_inside_tx[self.function]['ip']
            metadata_string = self.shadow_table.fetch(redis_key, self_ip)
            if metadata_string:
                repair_metadata = json.loads(metadata_string)
                self.RYW_table = repair_metadata['RYW']
                self.downstream_func_table = repair_metadata['downstream']
                self.upstream_func_table = repair_metadata['upstream']
                self.dirty = repair_metadata['dirty']
            self.function_pos_whole_batch = json.loads(self.shadow_table.fetch(f"{batch_id}:POS::", self_ip))
            self.repair_metadata_fetched = True
            # modify parent_cnt
            self.parent_cnt = self.downstream_func_table.get("up_cnt", 0) + self.RYW_table.get('up_cnt', 0) + self.parent_cnt

    def check_runnable(self, is_repair, no_parent_execution):
        # not in repair mode, check is finished outside the container.
        if not is_repair or no_parent_execution or not self.fast_path_enabled:
            return True
        else:
            self.parent_executed += 1
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

    def fin_repair(self, batch_id, ip):
        logging.info(f"Finishing repair: {self.function}")
        url = f'http://{ip}:7000/fin_repair'
        data = {'batch_id': batch_id}
        requests.post(url, json=data)

    def run(self, batch_id, transaction_id, is_repair):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": self.write_set, 
                                "RYW_subjection": {},
                                "downstream_func_table": self.downstream_func_table, 
                                "function_pos_whole_batch":self.function_pos_whole_batch,
                                "dirty": self.dirty,
                               }
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        if not self.fast_path_enabled or not is_repair or self.dirty:
            store = Store(self.workflow, self.function, transaction_id, self.input, self.output, self.function_pos_inside_tx, self.shadow_table, self.cache, TxMetaData_thisFunc, is_repair)
            self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}

            # pre-exec
            exec(self.code, self.ctx)

            # run function
            out = eval('main()', self.ctx)
        # in repair mode and in fast-path: trigger next function inside the container.
        logging.info(f"Trigger Next functions: {self.next_functions}, RYW:{self.RYW_table.get('down_funcs', [])}, crosstx:{self.upstream_func_table}")
        if self.fast_path_enabled and is_repair:
            next_trigger_tasks = []
            for next_func in self.next_functions:
                if next_func == 'END':
                    self_ip = self.function_pos_inside_tx[self.function]['ip']
                    next_trigger_tasks.append(gevent.spawn(self.fin_repair, batch_id, self_ip))
                    break
                ip = self.function_pos_whole_batch[self.transaction_id][next_func]['ip']
                port = self.function_pos_whole_batch[self.transaction_id][next_func]['port']
                logging.info(f"Next function: {next_func}, batch_id: {batch_id}, ip: {ip}, port: {port}")
                next_trigger_tasks.append(gevent.spawn(self.trigger_next_function, batch_id, self.transaction_id, ip, port, self.dirty))
            for RYW_downstream_func in self.RYW_table.get('down_funcs', []):
                ip = self.function_pos_whole_batch[self.transaction_id][RYW_downstream_func]['ip']
                port = self.function_pos_whole_batch[self.transaction_id][RYW_downstream_func]['port']
                logging.info(f"RYW Next function: {RYW_downstream_func}, batch_id: {batch_id}, ip: {ip}, port: {port}")
                next_trigger_tasks.append(gevent.spawn(self.trigger_next_function, batch_id, self.transaction_id, ip, port, self.dirty))
            for downstream_func_info in self.upstream_func_table:
                downstream_func = downstream_func_info['function_name']
                downstream_transaction_id =  downstream_func_info['transaction_id']
                ip = self.function_pos_whole_batch[downstream_transaction_id][downstream_func]['ip']
                port = self.function_pos_whole_batch[downstream_transaction_id][downstream_func]['port']
                logging.info(f"CrossTX Next function: {downstream_func}, batch_id: {batch_id}, ip: {ip}, port: {port}")
                next_trigger_tasks.append(gevent.spawn(self.trigger_next_function, batch_id, downstream_transaction_id, ip, port, True))
            gevent.joinall(next_trigger_tasks)

        io_latency = 0
        if not self.fast_path_enabled or not is_repair or self.dirty:
            io_latency = store.io_latency

        return TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency


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
    runner.init(inp['workflow'], inp['function'],inp['node_list'], inp['fast_path_enabled'])

    proxy.status = 'ok'
    return ('OK', 200)


@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'

    inp = request.get_json(force=True, silent=True)
    is_repair = inp['repair']
    transaction_id = inp['transaction_id']
    batch_id = ""
    no_parent_execution = False
    io_latency = 0
    rs, ws, RYW_subjection={},{},{}
    # first run, or not the reserved container. Save the info for this container.
    if not is_repair:
        input = inp['input']
        output = inp['output']
        function_pos = inp['function_pos']
        write_set = inp['write_set'] 
        next_functions = inp['next_functions']
        parent_cnt = inp['parent_cnt']
        runner.save(transaction_id, input, output, function_pos, write_set, next_functions, parent_cnt)
    else:
        batch_id = inp['batch_id']
        if runner.fast_path_enabled:
            dirty = inp.get('dirty', False)
            no_parent_execution = inp.get('no_parent_execution', False)
            # get the info from redis
            runner.fetch_repair_metadata(batch_id, transaction_id, dirty)
        
    # record the execution time
    logging.info(f"is_repair:{is_repair}, no_parent_execution:{no_parent_execution}")
    if runner.check_runnable(is_repair, no_parent_execution):
        rs, ws, RYW_subjection,io_latency = runner.run(batch_id, transaction_id, is_repair)

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
