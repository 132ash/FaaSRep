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
import traceback

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

OPT_REPAIR = container_config.OPT_REPAIR
PESSI_REPAIR = container_config.PESSI_REPAIR
db_server = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)

default_file = 'main.py'
work_dir = '/proxy'


def post_json(url, data, context):
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        logging.error("[HTTP ERROR] %s: %s: %s", context, url, exc)
        return None

def join_and_report(jobs, context):
    if not jobs:
        return True
    gevent.joinall(jobs)
    ok = True
    for job in jobs:
        if job.exception is not None:
            ok = False
            logging.error("[GEVENT ERROR] %s: %s", context, job.exception)
    return ok


def should_cleanup_repair_context(is_repair, fast_path_enabled, optimistic_repair, repair_mode):
    if not is_repair:
        return False
    if not fast_path_enabled:
        return True
    if not optimistic_repair:
        return True
    return repair_mode == PESSI_REPAIR

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
        
        # Transaction contexts
        self.tx_contexts = {}
        self.tx_contexts_lock = gevent.lock.BoundedSemaphore()

        # infomation saved in first run
        self.input = {}
        self.output = {}
        self.parent_cnt = 0

        # system mode options
        self.fast_path_enabled = False
        self.remote_lock_enabled = False
        self.optimistic_repair = False

    def init(self, host_addr, workflow, function, sink, validator, node_list,input,output,parent_cnt,function_pos, port, fast_path_enabled, optimistic_repair):
        # update function status
        self.host_addr = host_addr
        self.workflow = workflow
        self.function = function
        self.sink_addr = sink
        self.validator_addr = validator
        self.input = input
        self.output = output
        self.parent_cnt = parent_cnt
        self.function_pos = function_pos
        self.port = port
        self.fast_path_enabled = fast_path_enabled
        self.optimistic_repair = optimistic_repair
        # shadow table on each host
        self.shadow_table = RedisShadowTable(node_list, container_config.REDIS_PORT, container_config.REDIS_SHADOW_TABLE_DB, self.host_addr, db_server)
        # local cache
        self.cache = RedisCache(container_config.REDIS_CACHE_PORT, container_config.REDIS_CACHE_DB, db_server)
        self.repair_sidecar = RepairSidecar(self.function, self.shadow_table, self.cache, self.function_pos, self.port, db_server)
        self.container_state = RUNNING
        os.chdir(work_dir)

        # compile first
        filename = os.path.join(work_dir, default_file)
        with open(filename, 'r') as f:
            self.code = compile(f.read(), filename, mode='exec')
        store.init(self.function, self.shadow_table, self.cache, db_server, self.fast_path_enabled, self.function_pos, self.validator_addr)

    def get_context(self, transaction_id):
        self.tx_contexts_lock.acquire()
        ctx = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        return ctx

    def save(self, transaction_id, write_set):
        self.tx_contexts_lock.acquire()
        if transaction_id not in self.tx_contexts:
            self.tx_contexts[transaction_id] = {
                'write_set': write_set,
                'container_state': RUNNING,
                'parent_executed': 0,
                'repair_mode': None,
                'dirty': False,
                'keys_from_upstream': {},
                'keys_from_RYW': {},
                'subjection_waiting_cnt': 0,
                'batch_id': '',
                'repair_metadata_lock': gevent.lock.BoundedSemaphore(),
                'successor_port': {}
            }
        else:
            self.tx_contexts[transaction_id]['write_set'] = write_set
            self.tx_contexts[transaction_id]['container_state'] = RUNNING
        self.tx_contexts_lock.release()

    def prepair_subjection_before_repair(self, transaction_id, ctx):
        set_pipeline = self.shadow_table.redis[self.host_addr].pipeline() 
        set_pipeline.multi()
        upstream_fetch_results = self.repair_sidecar.fetch_upstream_keys(ctx['keys_from_upstream'], transaction_id)
        waiting_cnt = 0
        for _, upstream_tx_dict in upstream_fetch_results.items():
            for _, upstream_func_dict in upstream_tx_dict.items():
                for _, func_result_info in upstream_func_dict.items():
                    if func_result_info['state'] is None:
                        # If the state is None, it means the function has been commited. trigger cache update.
                        for key in func_result_info['fetched_keys'].keys():
                            ctx['keys_from_upstream'].pop(key)
                            self.cache.update_and_fetch(key)
                    elif func_result_info['state'] == RUNNING:
                        # If upstream function is still running, this func should wait for it.
                        waiting_cnt += 1
                    elif func_result_info['state'] == REPAIRED:
                        # update the fetched keys in shadow table. add fetch count.
                        upstream_key_prefix = f"{transaction_id}:UPSTREAM:{self.function}:"
                        for key, value in func_result_info['fetched_keys'].items():
                            set_pipeline.set(upstream_key_prefix+key, value)
        set_pipeline.execute()
        return waiting_cnt

    # in repair mode: save the repair metadata. 
    def fetch_repair_metadata(self, transaction_id, repair_mode, metadata_nofast={}):
        self.tx_contexts_lock.acquire()
        ctx = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        if not ctx:
            return

        ctx['repair_metadata_lock'].acquire()
        if ctx['repair_mode'] == None or ctx['repair_mode'] == OPT_REPAIR and repair_mode == PESSI_REPAIR:
            ##print(f"repair mode changed from {self.repair_mode} to {repair_mode}")
            ctx['repair_mode'] = repair_mode
            ctx['parent_executed'] = 0
            # not fast-path: the metadata is sent by workersp.
            if not self.fast_path_enabled:
                ctx['keys_from_upstream'] = metadata_nofast.get('upstream_keys', {})
                ctx['keys_from_RYW'] = metadata_nofast.get('RYW_keys', {})
                ctx['dirty'] = metadata_nofast.get('dirty', False)
                ##print(f"NOFAST Fetched repair metadata: keys_from_upstream: {self.keys_from_upstream}, dirty: {self.dirty}, subjection_waiting_cnt:{self.subjection_waiting_cnt}", flush=True)
            else:
                metadata_string = self.shadow_table.raw_fetch_data( f"{transaction_id}:REPAIR_{ctx['repair_mode']}:{self.function}:", self.host_addr)
                if metadata_string:
                    repair_metadata = json.loads(metadata_string)
                    ctx['keys_from_upstream'] = repair_metadata['upstream_keys']
                    ctx['keys_from_RYW'] = repair_metadata['RYW_keys']
                    ctx['successor_port'] = repair_metadata['successor_port']
                    ctx['dirty'] = repair_metadata['dirty']
                    # besides metadata, the subjection from upstream should be prepared by container itself.
                    ctx['subjection_waiting_cnt'] = self.prepair_subjection_before_repair(transaction_id, ctx)
                    ##print(f"FASTPATH Fetched repair metadata: keys_from_upstream: {self.keys_from_upstream}, dirty: {self.dirty}, subjection_waiting_cnt:{self.subjection_waiting_cnt}", flush=True)
        ctx['repair_metadata_lock'].release()

    def check_runnable(self, transaction_id, is_repair, no_parent_execution, repair_mode_from_upstream):
        # not in repair mode, check is finished outside the container.
        if not is_repair or not self.fast_path_enabled:
            ##print(f"Check runnable: not in repair mode or not fast-path enabled, is_repair: {is_repair}, fast_path_enabled: {self.fast_path_enabled}", flush=True)
            return True
        else:
            self.tx_contexts_lock.acquire()
            ctx = self.tx_contexts.get(transaction_id)
            self.tx_contexts_lock.release()
            if not ctx:
                return False

            if ctx['repair_mode'] == PESSI_REPAIR and repair_mode_from_upstream != PESSI_REPAIR:
                # in pessi repair mode: only the request from upstream in the workflow should be accepted.
                ##print(f"Rejecting request from upstream: {repair_mode_from_upstream} != {PESSI_REPAIR}", flush=True)
                return False
            if not no_parent_execution:
                ctx['repair_metadata_lock'].acquire()
                ctx['parent_executed'] += 1
                ctx['repair_metadata_lock'].release()
            ##print(f"Parent executed: {self.parent_executed}, parent_cnt: {self.parent_cnt}, waiting_cnt: {self.subjection_waiting_cnt}", flush=True)
            return ctx['parent_executed'] == self.parent_cnt + ctx['subjection_waiting_cnt']
        
    def trigger_next_function(self, transaction_id, ip, port ,dirty=False, batch_id="", repair_mode=''):
        url = f'http://{ip}:{port}/run'
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'repair': True,
            'repair_mode':repair_mode
            }
        ##print(f"Triggering next function: {ip}:{port}, batch_id: {batch_id}, transaction_id: {transaction_id}, dirty: {dirty}", flush=True)
        post_json(url, data, f"trigger next {transaction_id}")

    def fin_repair(self, batch_id, transaction_id, repair_mode):
        url = f'http://{self.sink_addr}/fin_repair'
        logging.info(f"Finishing repair: notifying sink at {self.sink_addr} for transaction {transaction_id} batch {batch_id} mode {repair_mode}")
        data = { 'workflow_name':self.workflow  ,'batch_id': batch_id, "transaction_id": transaction_id, 'repair_mode':repair_mode}
        post_json(url, data, f"finish repair {transaction_id}")

    def abort_transaction(self, batch_id, transaction_id, repair_mode, msg=""):
        logging.info(f"Aborting transaction: notifying sink at {self.sink_addr} for transaction {transaction_id} batch {batch_id} mode {repair_mode} with message: {msg}")
        url = f'http://{self.sink_addr}/abort'
        data = {'batch_id': batch_id, "transaction_id": transaction_id, 'workflow_name': self.workflow, 'repair':True, 'repair_mode':repair_mode, "error": msg}
        post_json(url, data, f"abort repair {transaction_id}")

    def trigger_downstream_functions(self, batch_id, aborted, downstream_funcs, repair_mode, transaction_id, ctx):
        ##print(f"Trigger waiting functions in opt: {downstream_funcs}", flush=True)
        next_trigger_tasks = []
        # Trigger all waiting downstream functions
        for func_info in downstream_funcs:
            downstream_txid, ip, port = func_info[0], func_info[2], func_info[3]
            next_trigger_tasks.append(
            gevent.spawn(
                self.trigger_next_function,
                downstream_txid, ip, port
                )
            )
        # If not aborted, trigger successor functions in workflow graph.
        if not aborted:
            for next_func, port in ctx['successor_port'].items():
                if next_func == 'END':
                    next_trigger_tasks.append(
                        gevent.spawn(self.fin_repair, batch_id, transaction_id, repair_mode)
                    )
                    break
                next_ip = self.function_pos[next_func]
                logging.info(f"Triggering successor function: {next_func} at {next_ip}:{port} for transaction {transaction_id} batch {batch_id} repair_mode {repair_mode}")
                next_trigger_tasks.append(
                    gevent.spawn(
                    self.trigger_next_function,
                    transaction_id, next_ip, port, ctx['dirty'], batch_id, repair_mode
                    )
                )
        join_and_report(next_trigger_tasks, f"trigger downstream {transaction_id}")

    def run(self, transaction_id, is_repair):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.
        self.tx_contexts_lock.acquire()
        ctx_data = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        if not ctx_data:
            return True, json.dumps({'Abort': True, 'error': "Transaction context not found"}), {}, {}, {}, 0

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": ctx_data['write_set'], 
                                "RYW_subjection": {},
                                "keys_from_RYW": ctx_data['keys_from_RYW'],
                                "keys_from_upstream": ctx_data['keys_from_upstream'],
                              }
        aborted = False
        msg = ''
        
        # not in fast-path mode, not in repair mode or the fucntion is dirty: need re-run.
        ##print(f"Running function: {self.function}, transaction_id: {transaction_id}, is_repair: {is_repair}, dirty: {self.dirty}, fast_path_enabled: {self.fast_path_enabled},write_set: {self.write_set}, parent_cnt: {self.parent_cnt}, repair_metadata:{TxMetaData_thisFunc}", flush=True)
        # need run: first run / repair, in fast-path and dirty / repair, not in fast-path.
        if not is_repair or not self.fast_path_enabled or ctx_data['dirty']:
            # Create a new store for this run
            current_store = Store()
            current_store.init(self.function, self.shadow_table, self.cache, db_server, self.fast_path_enabled, self.function_pos, self.validator_addr)
            
            current_store.runtime_init(self.input, self.output, is_repair, transaction_id, TxMetaData_thisFunc)
            local_ctx = {'workflow_name': self.workflow, 'function_name': self.function, 'store': current_store}
            # pre-exec
            try:
                exec(self.code, local_ctx)
        # run function
                out = eval('main()', local_ctx)               
            except Exception as e:
                aborted = True
                error_traceback = traceback.format_exc()
                msg = json.dumps({'Abort': True, 'error': str(e), 'traceback': error_traceback})
                #print(f"Function {self.function} execution failed with traceback:\n{error_traceback}", flush=True)
        # the function finished repair, not abort, send data to waiting functions in fastpath..
        if is_repair:
            if self.fast_path_enabled:
                if aborted:
                    ctx_data['container_state'] = ABORTED
                    self.abort_transaction(ctx_data['batch_id'], transaction_id, ctx_data['repair_mode'], msg)
                else:
                    ctx_data['container_state'] = REPAIRED
                    # in fast-path: the container need to trigger downstream functions.
                    # besides, in optimistic repair mode, the container should send data to waiting downstream functions.
                    if self.optimistic_repair:
                        # optimistic repair: the container should send data to waiting downstream functions.
                        downstream_funcs_opt = self.repair_sidecar.set_state_and_get_waiting_downstream(transaction_id, ctx_data['container_state'])
                        self.repair_sidecar.send_data_to_waiting_downstream(transaction_id, downstream_funcs_opt)
                    else:
                        downstream_funcs_opt = []
                    self.trigger_downstream_functions(ctx_data['batch_id'], aborted, downstream_funcs_opt, ctx_data['repair_mode'], transaction_id, ctx_data)   
            else:
                ctx_data['repair_mode'] = None     

        io_latency = 0

        if not is_repair:
            io_latency = current_store.io_latency
        
        # Keep optimistic fast-path context so the same container can accept a later
        # cascaded pessimistic retry for the same transaction.
        if should_cleanup_repair_context(
            is_repair,
            self.fast_path_enabled,
            self.optimistic_repair,
            ctx_data['repair_mode'],
        ):
            self.tx_contexts_lock.acquire()
            if transaction_id in self.tx_contexts:
                logging.info(
                    "[REPAIR CONTEXT CLEANUP] workflow=%s function=%s tx=%s batch=%s mode=%s",
                    self.workflow,
                    self.function,
                    transaction_id,
                    ctx_data['batch_id'],
                    ctx_data['repair_mode'],
                )
                del self.tx_contexts[transaction_id]
            self.tx_contexts_lock.release()
        elif is_repair:
            logging.info(
                "[REPAIR CONTEXT RETAINED] workflow=%s function=%s tx=%s batch=%s mode=%s",
                self.workflow,
                self.function,
                transaction_id,
                ctx_data['batch_id'],
                ctx_data['repair_mode'],
            )

        return aborted, msg, TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency


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
    runner.init(inp['host_addr'], inp['workflow'], inp['function'], inp['sink'], inp['validator'],
                inp['node_list'], inp['input'],inp['output'],inp['parent_cnt'],
                inp['function_pos'],inp['port'],inp['fast_path_enabled'], 
                inp['optimistic_repair'])
    
    proxy.status = 'ok'
    return ('OK', 200)

@proxy.route('/run', methods=['POST'])
def run():
    proxy.status = 'run'
    inp = request.get_json(force=True, silent=True)
    transaction_id = inp['transaction_id']
    is_repair = inp.get('repair', False)
    repair_mode = inp.get('repair_mode', None)
    batch_id = inp.get('batch_id', '')
    
    no_parent_execution = False

    if not is_repair:
        runner.save(transaction_id, inp.get('write_set', {}))
    else:
        logging.info(f"Received repair request: workflow={runner.workflow} function={runner.function} tx={transaction_id} batch={batch_id} mode={repair_mode}")
        if batch_id:
            ctx = runner.get_context(transaction_id)
            if ctx:
                ctx['batch_id'] = batch_id
            else:
                logging.error(
                    "[REPAIR CONTEXT MISSING] workflow=%s function=%s tx=%s batch=%s mode=%s",
                    runner.workflow,
                    runner.function,
                    transaction_id,
                    batch_id,
                    repair_mode,
                )
            
            if runner.fast_path_enabled:
                no_parent_execution = inp.get('no_parent_execution', False)
            runner.fetch_repair_metadata(transaction_id, repair_mode, inp.get('repair_states', {}))
        else:
            ctx = runner.get_context(transaction_id)
            if ctx and ctx['repair_mode'] == PESSI_REPAIR:
                logging.warning(
                    "[PESSI REPAIR WITHOUT BATCH ID] workflow=%s function=%s tx=%s mode=%s",
                    runner.workflow,
                    runner.function,
                    transaction_id,
                    repair_mode,)
                return json.dumps({'Abort': True, 'error': "PESSIMISTIC REPAIR should not be triggered without batch_id."})

    if runner.check_runnable(transaction_id, is_repair, no_parent_execution, repair_mode):
        logging.info(f"Running function for transaction {transaction_id} batch {batch_id} repair_mode {repair_mode}")
        aborted, abort_msg, rs, ws, RYW_subjection, io_latency = runner.run(transaction_id, is_repair)
        if aborted:
            return abort_msg
        
        res = {
            "read_set": rs,
            "write_set": ws,
            "RYW_subjection": RYW_subjection,
            "io_latency": io_latency
        }
        proxy.status = 'ok'
        return res

    if is_repair:
        logging.info(
            "[REPAIR WAITING] workflow=%s function=%s tx=%s batch=%s mode=%s no_parent_execution=%s",
            runner.workflow,
            runner.function,
            transaction_id,
            batch_id,
            repair_mode,
            no_parent_execution,
        )
    return ('OK', 200)

if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy, backlog=container_config.HTTP_SERVER_BACKLOG)
    server.serve_forever()
