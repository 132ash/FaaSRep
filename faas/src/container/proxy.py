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
from redis_component import RedisShadowTable, RedisCache, RepairSidecar

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
container_state = RUNNING
db_server = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)


default_file = 'main.py'
work_dir = '/proxy'

class Runner:
    def __init__(self):
        self.code = None
        self.workflow = None
        self.function = None
        self.node_list = None
        self.input = None
        self.output = None
        self.ip = None
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
        self.parent_cnt = None

        self.parent_executed = 0
        self.upstream_waiting = 0

        # infomation fetched from Redis in repair
        self.repair_metadata_lock = gevent.lock.BoundedSemaphore()
        self.repair_metadata_fetched = False
        self.dirty = False
        self.upstream_func_count = 0
        self.upstream_fetched = 0
        self.keys_from_upstream = {}
        self.keys_from_RYW = {}

        # fast path enabled
        self.fast_path_enabled = False
        # remote lock enabled
        self.remote_lock_enabled = False
        self.lock_set = {}

        # function state: 
        self.state = ''


    def init(self, workflow, function, node_list,input,output,ip, port, fast_path_enabled, remote_lock_enabled):
        print('init...')

        # update function status
        self.workflow = workflow
        self.function = function
        self.node_list = node_list
        self.input = input
        self.output = output
        self.ip = ip
        self.port = port
        self.fast_path_enabled = fast_path_enabled
        self.remote_lock_enabled = remote_lock_enabled
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB, self.ip)
        # local cache
        self.cache = RedisCache(container_config.REDIS_PORT, container_config.REDIS_CACHE_DB, db_server)
        self.repair_sidecar = RepairSidecar(self.function, self.shadow_table, self.cache, self.ip, self.port)

        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.function, self.shadow_table, self.cache, db_server, self.fast_path_enabled, self.remote_lock_enabled)

        print('init finished...')

    def save(self, transaction_id, function_pos, write_set, parent_cnt, lock_set):
        self.transaction_id = transaction_id
        self.function_pos_inside_tx = function_pos
        self.write_set = write_set
        self.parent_cnt = parent_cnt
        self.lock_set = lock_set

    def updated_by_upstream(self, state):
        if state == REPAIRED:
            self.upstream_waiting -= 1
        elif state == ABORTED:
            self.become_pessimistic()

    def become_pessimistic(self):
        # TODO: jobs when visit aborted function, and what workersp do under different situations.
        # fetch remote state and share between different containers? 
        pass

    def prepair_subjection_before_repair(self, transaction_id):
        upstream_fetch_results = self.repair_sidecar.fetch_upstream_keys(self.keys_from_upstream, transaction_id)
        for _, upstream_tx_dict in upstream_fetch_results.items():
            for upstream_txid, upstream_func_dict in upstream_tx_dict.items():
                for upstream_func, func_result_info in upstream_func_dict.items():
                    if func_result_info['state'] is None:
                        # If the state is None, it means the function has been commited. trigger cache update.
                        for key in func_result_info['fetched_keys'].keys():
                            self.keys_from_upstream.pop(key)
                            self.cache.update_and_fetch(key)
                    elif func_result_info['state'] == RUNNING:
                        # If upstream function is still running, this func should wait for it.
                        self.upstream_waiting += 1
                    elif func_result_info['state'] == REPAIRED:
                        # update the fetched keys in shadow table. add fetch count.
                        self.upstream_fetched += len(func_result_info['fetched_keys'])
                        upstream_key_prefix = f"{transaction_id}:{self.function}:UPSTREAM:"
                        for key, value in func_result_info['fetched_keys'].items():
                            self.shadow_table.put(upstream_key_prefix+key, self.shadow_table.ip, value)
                    elif func_result_info['state'] == ABORTED:
                        self.become_pessimistic()

    def fetch_repair_metadata(self, transaction_id, metadata_norepair={}):
        self.repair_metadata_lock.acquire()
        if not self.repair_metadata_fetched:
            if not self.fast_path_enabled:
                self.upstream_func_count = metadata_norepair['up_cnt']
                self.keys_from_upstream = metadata_norepair['upstream_keys']
                self.keys_from_RYW = metadata_norepair['RYW_keys']
                self.successor_pos = metadata_norepair['successor_pos']
                self.dirty = metadata_norepair['dirty']
            else:
                self.repair_metadata_fetched = True
                try:
                    metadata_string = self.shadow_table.raw_fetch_data( f"{transaction_id}:REPAIR:{self.function}:", self.self_ip)
                except KeyError:
                    metadata_string = None
                if metadata_string:
                    repair_metadata = json.loads(metadata_string)
                    self.upstream_func_count = repair_metadata['up_cnt']
                    self.keys_from_upstream = repair_metadata['upstream_keys']
                    self.keys_from_RYW = repair_metadata['RYW_keys']
                    self.successor_pos = repair_metadata['successor_pos']
                    self.dirty = repair_metadata['dirty']
                self.parent_cnt += self.upstream_func_count
                logging.info(f"Fetched repair metadata: upstream_func_count: {self.upstream_func_count}, keys_from_upstream: {self.keys_from_upstream}, dirty: {self.dirty}")
        self.repair_metadata_lock.release()

    def check_runnable(self, is_repair, no_parent_execution):
        # not in repair mode, check is finished outside the container.
        if not is_repair or no_parent_execution or not self.fast_path_enabled:
            return True
        else:
            self.parent_executed += 1
            logging.info(f"Parent executed: {self.parent_executed}, parent_cnt: {self.parent_cnt}")
            return self.parent_executed == self.parent_cnt
        
    def trigger_next_function(self, transaction_id, ip, port,state ,dirty=False, batch_id=""):
        url = f'http://{ip}:{port}/run'
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'repair': True,
            'dirty':dirty,
            'function': self.function,
            'up_state': state,  # state of the upstream function
            }
        logging.info(f"Triggering next function: {ip}:{port}, batch_id: {batch_id}, transaction_id: {transaction_id}, dirty: {dirty}")
        requests.post(url, json=data)

    def fin_repair(self, batch_id, transaction_id, ip):
        logging.info(f"Finishing repair: {self.function}")
        url = f'http://{ip}:7000/fin_repair'
        data = {'batch_id': batch_id, "transaction_id": transaction_id}
        requests.post(url, json=data)

    def trigger_downstream_functions(self, batch_id, aborted, downstream_funcs):
        logging.info(f"Trigger waiting functions: {downstream_funcs}")
        next_trigger_tasks = []
        # Trigger all waiting downstream functions
        for func_info in downstream_funcs:
            ip, port = func_info[2], func_info[3]
            next_trigger_tasks.append(
            gevent.spawn(
                self.trigger_next_function,
                self.transaction_id, ip, port, container_state, self.dirty, batch_id
                )
            )
        # If not aborted, trigger successor functions in workflow graph.
        if not aborted:
            for next_func, pos in self.successor_pos:
                if next_func == 'END':
                    next_trigger_tasks.append(
                        gevent.spawn(self.fin_repair, batch_id, self.transaction_id, self.ip)
                    )
                    break
                logging.info(f"Trigger Next functions: {self.successor_pos}")
                next_trigger_tasks.append(
                    gevent.spawn(
                    self.trigger_next_function,
                    self.transaction_id, pos['ip'], pos['port'], container_state, self.dirty, batch_id
                    )
                )
        gevent.joinall(next_trigger_tasks)

    def run(self, batch_id, transaction_id, is_repair):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": self.write_set, 
                                "lock_set": self.lock_set,
                                "RYW_subjection": {},
                                "keys_from_RYW": self.keys_from_RYW,
                                "keys_from_upstream": self.keys_from_upstream,
                              }
        aborted = False
        msg = ''
        
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        logging.info(f"Running function: {self.function}, transaction_id: {transaction_id}, is_repair: {is_repair}, dirty: {self.dirty}, fast_path_enabled: {self.fast_path_enabled}, input: {self.input}, output: {self.output}, function_pos_inside_tx: {self.function_pos_inside_tx}, write_set: {self.write_set}, next_functions: {self.next_functions}, parent_cnt: {self.parent_cnt}, lock_set:{self.lock_set}")
        if is_repair:
            self.prepair_subjection_before_repair(transaction_id)
        if not self.fast_path_enabled or not is_repair or self.dirty:
            store.runtime_init(self.input, self.output, is_repair, self.function_pos_inside_tx, transaction_id, TxMetaData_thisFunc)
            self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}
            # pre-exec
            try:
                exec(self.code, self.ctx)
                # run function
                out = eval('main()', self.ctx)               
            except Exception as e:
                aborted = True
                msg = json.dumps({'Abort': True, 'error': str(e), 'lock_set': self.lock_set})
        # in repair mode and in fast-path: trigger next function inside the container.
        if is_repair:
            container_state = ABORTED if aborted else REPAIRED
            downstream_funcs = self.repair_sidecar.set_state_and_get_waiting_downstream(transaction_id, container_state)
            if self.fast_path_enabled:
                if container_state == REPAIRED:
                    self.repair_sidecar.send_data_to_waiting_downstream(transaction_id, downstream_funcs)
                self.trigger_downstream_functions(batch_id, aborted, downstream_funcs)
                downstream_funcs = {}

        io_latency = 0
        lock_latency = 0
        if self.remote_lock_enabled:
            lock_latency = store.lock_latency

        if not self.fast_path_enabled or not is_repair or self.dirty:
            io_latency = store.io_latency

        return aborted, msg, TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency, lock_latency, downstream_funcs


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
    runner.init(inp['workflow'], inp['function'],inp['node_list'], inp['input'],inp['output'],inp['ip'],inp['port'],inp['fast_path_enabled'], inp['remote_lock_enabled'])

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
        lock_set = inp['lock_set']
        function_pos = inp['function_pos'] # function pos up to this function
        write_set = inp['write_set'] # upstream write set
        parent_cnt = inp['parent_cnt']
        runner.save(transaction_id, function_pos, write_set, parent_cnt, lock_set)
    else:
        batch_id = inp['batch_id']
        state = inp['up_state']
        upstream_transaction_id = inp.get('transaction_id', '')
        upstream_function = inp.get('function', '')
        runner.updated_by_upstream(state, upstream_transaction_id, upstream_function)
        if runner.fast_path_enabled:
            no_parent_execution = inp.get('no_parent_execution', False)
            # get the info from redis
        runner.fetch_repair_metadata(transaction_id, inp)
        
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    if runner.check_runnable(is_repair, no_parent_execution):
        aborted, abort_msg, rs, ws, RYW_subjection, io_latency, lock_latency, downstream_funcs = runner.run(batch_id, transaction_id, is_repair)
        if aborted:
            return abort_msg

    res = {
        "read_set": rs,
        "write_set": ws,
        "RYW_upstreams": RYW_subjection,
        "io_latency": io_latency,
        "lock_set": lock_set,
        "lock_latency": lock_latency,
        'waiting_downstream': downstream_funcs
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
