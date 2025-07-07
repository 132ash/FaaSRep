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
db_server = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)

default_file = 'main.py'
work_dir = '/proxy'

class Runner:
    def __init__(self):
        self.code = None
        self.workflow = None
        self.function = None
        self.node_list = None
        self.sink_addr = None
        self.function_pos = None
        self.shadow_table = None
        self.cache = None
        self.ctx = {}

        # infomation saved in first run
        self.transaction_id = None
        self.input = {}
        self.output = {}
        self.write_set = {}
        self.is_repair = None
        self.parent_cnt = 0
        self.parent_executed = 0

        # infomation fetched from Redis in repair
        self.repair_metadata_lock = gevent.lock.BoundedSemaphore()
        self.repair_metadata_fetched = False
        self.dirty = False
        self.keys_from_upstream = {}
        self.keys_from_RYW = {}

        # system mode options
        self.fast_path_enabled = False
        self.remote_lock_enabled = False
        self.optimistic_repair = False
        self.lock_set = {}

    def init(self, workflow, function, sink, node_list,input,output,function_pos, port, fast_path_enabled, remote_lock_enabled, optimistic_repair):
        # update function status
        self.workflow = workflow
        self.function = function
        self.sink_addr = sink
        self.node_list = node_list
        self.input = input
        self.output = output
        self.function_pos = function_pos
        self.port = port
        self.fast_path_enabled = fast_path_enabled
        self.remote_lock_enabled = remote_lock_enabled
        self.optimistic_repair = optimistic_repair
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB, self.ip)
        # local cache
        self.cache = RedisCache(container_config.REDIS_PORT, container_config.REDIS_CACHE_DB, db_server)
        self.repair_sidecar = RepairSidecar(self.function, self.shadow_table, self.cache, self.ip, self.port)
        self.container_state = RUNNING
        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.function, self.shadow_table, self.cache, db_server, self.fast_path_enabled, self.remote_lock_enabled, self.function_pos)

        logging.info('init finished...')

    def save(self, transaction_id, write_set, parent_cnt, lock_set):
        self.transaction_id = transaction_id
        self.write_set = write_set
        self.parent_cnt = parent_cnt
        self.lock_set = lock_set
        self.container_state = RUNNING

    def prepair_subjection_before_repair(self, transaction_id):
        set_pipeline = self.shadow_table[self.ip].pipeline() 
        set_pipeline.multi()
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
                        self.parent_cnt += 1
                    elif func_result_info['state'] == REPAIRED:
                        # update the fetched keys in shadow table. add fetch count.
                        upstream_key_prefix = f"{transaction_id}:{self.function}:UPSTREAM:"
                        for key, value in func_result_info['fetched_keys'].items():
                            set_pipeline.set(upstream_key_prefix+key, value)
        set_pipeline.execute()

    # in repair mode: save the repair metadata. 
    def fetch_repair_metadata(self, transaction_id, metadata_norepair={}):
        self.repair_metadata_lock.acquire()
        if not self.repair_metadata_fetched:
            self.repair_metadata_fetched = True
            # not fast-path: the metadata is sent by workersp.
            if not self.fast_path_enabled:
                self.keys_from_upstream = metadata_norepair['upstream_keys']
                self.keys_from_RYW = metadata_norepair['RYW_keys']
                self.dirty = metadata_norepair['dirty']
            else:
                try:
                    metadata_string = self.shadow_table.raw_fetch_data( f"{transaction_id}:REPAIR:{self.function}:", self.ip)
                except KeyError:
                    metadata_string = None
                if metadata_string:
                    repair_metadata = json.loads(metadata_string)
                    self.keys_from_upstream = repair_metadata['upstream_keys']
                    self.keys_from_RYW = repair_metadata['RYW_keys']
                    self.successor_port = repair_metadata['successor_port']
                    self.dirty = repair_metadata['dirty']
                    # besides metadata, the subjection from upstream should be prepared by container itself.
                    self.prepair_subjection_before_repair(transaction_id)
                    logging.info(f"FASTPATH Fetched repair metadata: keys_from_upstream: {self.keys_from_upstream}, dirty: {self.dirty}")
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

    def abort_transaction(self, batch_id, transaction_id):
        logging.info(f"abort transaction: {self.function}")
        url = f'http://{self.sink_addr}:7000/abort'
        data = {'batch_id': batch_id, "transaction_id": transaction_id, 'workflow_name': self.workflow}
        requests.post(url, json=data)

    def trigger_downstream_functions(self, batch_id, aborted, downstream_funcs):
        logging.info(f"Trigger waiting functions in opt: {downstream_funcs}")
        next_trigger_tasks = []
        # Trigger all waiting downstream functions
        for func_info in downstream_funcs:
            ip, port = func_info[2], func_info[3]
            next_trigger_tasks.append(
            gevent.spawn(
                self.trigger_next_function,
                self.transaction_id, ip, port, self.container_state, self.dirty, batch_id
                )
            )
        # If not aborted, trigger successor functions in workflow graph.
        if not aborted:
            for next_func, port in self.successor_port:
                next_ip = self.function_pos[next_func]
                if next_func == 'END':
                    next_trigger_tasks.append(
                        gevent.spawn(self.fin_repair, batch_id, self.transaction_id, self.ip)
                    )
                    break
                logging.info(f"Trigger Next functions: {next_ip}:{self.successor_port}")
                next_trigger_tasks.append(
                    gevent.spawn(
                    self.trigger_next_function,
                    self.transaction_id, next_ip, port, self.container_state, self.dirty, batch_id
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
        logging.info(f"Running function: {self.function}, transaction_id: {transaction_id}, is_repair: {is_repair}, dirty: {self.dirty}, fast_path_enabled: {self.fast_path_enabled}, input: {self.input}, output: {self.output}, write_set: {self.write_set}, parent_cnt: {self.parent_cnt}, lock_set:{self.lock_set}")
        # need run: first run / repair, in fast-path and dirty / repair, not in fast-path.
        if not is_repair or not self.fast_path_enabled or self.dirty:
            store.runtime_init(self.input, self.output, is_repair, transaction_id, TxMetaData_thisFunc)
            self.ctx = {'workflow': self.workflow, 'function': self.function, 'store': store}
            # pre-exec
            try:
                exec(self.code, self.ctx)
                # run function
                out = eval('main()', self.ctx)               
            except Exception as e:
                aborted = True
                msg = json.dumps({'Abort': True, 'error': str(e), 'lock_set': self.lock_set})
        # the function finished repair, not abort, send data to waiting functions in fastpath..
        if is_repair:
            runner.repair_metadata_fetched = False
            runner.parent_cnt = 0
            runner.parent_executed = 0
            if self.fast_path_enabled:
                if aborted:
                    self.container_state = ABORTED
                    self.abort_transaction(batch_id, transaction_id)
                else:
                    self.container_state = REPAIRED
                    # in fast-path: the container need to trigger downstream functions.
                    # besides, in optimistic repair mode, the container should send data to waiting downstream functions.
                    if self.fast_path_enabled:
                        if self.optimistic_repair:
                            # optimistic repair: the container should send data to waiting downstream functions.
                            downstream_funcs_opt = self.repair_sidecar.set_state_and_get_waiting_downstream(transaction_id, self.container_state)
                            self.repair_sidecar.send_data_to_waiting_downstream(transaction_id, downstream_funcs_opt)
                        else:
                            downstream_funcs_opt = []
                        self.trigger_downstream_functions(batch_id, aborted, downstream_funcs_opt)        

        io_latency = 0
        lock_latency = 0
        if self.remote_lock_enabled:
            lock_latency = store.lock_latency

        if not self.fast_path_enabled or not is_repair or self.dirty:
            io_latency = store.io_latency

        return aborted, msg, TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency, lock_latency


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
    runner.init(inp['workflow'], inp['function'], inp['sink'],
                inp['node_list'], inp['input'],inp['output'],
                inp['function_pos'],inp['port'],inp['fast_path_enabled'], 
                inp['remote_lock_enabled'], inp['optimistic_repair'])

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
    # set the state to running.
    if not is_repair:
        lock_set = inp['lock_set']
        runner.save(transaction_id, inp['function_pos'], inp['write_set'], inp['parent_cnt'], lock_set)
    else:
        batch_id = inp['batch_id']
        if runner.fast_path_enabled:
            no_parent_execution = inp.get('no_parent_execution', False)
        runner.fetch_repair_metadata(transaction_id, inp['repair_states'])
        
    # record the execution time
    # only in remote lock mode, catch the runtime error(lock failed)
    if runner.check_runnable(is_repair, no_parent_execution):
        aborted, abort_msg, rs, ws, RYW_subjection, io_latency, lock_latency = runner.run(batch_id, transaction_id, is_repair)
        if aborted:
            return abort_msg

    res = {
        "read_set": rs,
        "write_set": ws,
        "RYW_upstreams": RYW_subjection,
        "io_latency": io_latency,
        "lock_set": lock_set,
        "lock_latency": lock_latency,
    }

    proxy.status = 'ok'
    return res


if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy)
    server.serve_forever()
