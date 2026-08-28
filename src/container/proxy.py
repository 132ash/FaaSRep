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
import time
from pathlib import Path


EXPERIMENT_LOGGING_ENABLED = (
    os.environ.get('FAASNAP_EXPERIMENT_LOGGING', '1').lower()
    not in {'0', 'false', 'no', 'off'}
)
_LOGGING_DISABLED_LEVEL = logging.CRITICAL + 1


class ActiveExperimentFileHandler(logging.Handler):
    """Container-side equivalent of config.experiment_logging handler."""

    logging_root = Path('/logging')

    def __init__(self):
        super().__init__(level=logging.INFO)
        self.run_directory = None
        self.stream = None

    def emit(self, record):
        try:
            run_id = (self.logging_root / 'ACTIVE_EXPERIMENT').read_text(
                encoding='utf-8').strip()
            if not run_id or Path(run_id).name != run_id:
                return
            run_dir = self.logging_root / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            if run_dir != self.run_directory or self.stream is None:
                if self.stream is not None:
                    self.stream.close()
                self.run_directory = run_dir
                self.stream = (run_dir / 'container.log').open(
                    'a', encoding='utf-8')
            self.stream.write(self.format(record) + '\n')
            self.stream.flush()
        except OSError:
            # Logging must never alter transaction progress.
            return

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        super().close()

from flask import Flask, request
from gevent.pywsgi import WSGIServer
from Store import Store
import container_config
from redis_component import RedisShadowTable, RedisCache, RepairSidecar

# 容器内的根 logger 也按当前实验分流，不再输出到终端。
root_logger = logging.getLogger()
root_logger.handlers.clear()
if EXPERIMENT_LOGGING_ENABLED:
    root_logger.setLevel(logging.INFO)
    container_log_handler = ActiveExperimentFileHandler()
    container_log_handler.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s'))
    root_logger.addHandler(container_log_handler)
else:
    root_logger.setLevel(_LOGGING_DISABLED_LEVEL)

def log_event(event, **fields):
    if not EXPERIMENT_LOGGING_ENABLED:
        return
    payload = {'event': event, 'timestamp': time.time(), **fields}
    logging.info(json.dumps(payload, sort_keys=True, default=str))


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
        self.progress_reporter_started = False
        self.last_transition_timestamp = time.time()

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
        if not self.progress_reporter_started:
            self.progress_reporter_started = True
            gevent.spawn_later(10, self.report_progress)

    def report_progress(self):
        self.tx_contexts_lock.acquire()
        contexts = {
            tx_id: {
                'batch_id': ctx['batch_id'], 'repair_mode': ctx['repair_mode'],
                'repair_epoch': ctx['repair_epoch'], 'attempt_id': ctx['attempt_id'],
                'container_state': ctx['container_state'],
                'parent_executed': ctx['parent_executed'],
                'upstream_wait_count': ctx['subjection_waiting_cnt'],
            }
            for tx_id, ctx in self.tx_contexts.items()
        }
        self.tx_contexts_lock.release()
        log_event('CONTAINER_PROGRESS_SNAPSHOT', workflow=self.workflow,
                  batch_id='', tx_id='', function=self.function,
                  repair_mode='', repair_epoch=0, attempt_id='',
                  state_before='', state_after='',
                  active_transactions=contexts,
                  last_transition_timestamp=self.last_transition_timestamp)
        gevent.spawn_later(10, self.report_progress)

    def get_context(self, transaction_id):
        self.tx_contexts_lock.acquire()
        ctx = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        return ctx

    def clear_context(self, transaction_id):
        self.tx_contexts_lock.acquire()
        removed = self.tx_contexts.pop(transaction_id, None)
        self.tx_contexts_lock.release()
        return removed is not None

    def save(self, transaction_id, write_set):
        self.last_transition_timestamp = time.time()
        self.tx_contexts_lock.acquire()
        if transaction_id not in self.tx_contexts:
            self.tx_contexts[transaction_id] = {
                'write_set': write_set,
                'container_state': RUNNING,
                'parent_executed': 0,
                'repair_mode': None,
                'repair_epoch': 0,
                'attempt_id': '',
                'dirty': False,
                'keys_from_upstream': {},
                'keys_from_RYW': {},
                'subjection_waiting_cnt': 0,
                'batch_id': '',
                'repair_metadata_lock': gevent.lock.BoundedSemaphore(),
                'execution_lock': gevent.lock.BoundedSemaphore(),
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
    def fetch_repair_metadata(self, transaction_id, repair_mode, repair_epoch=1, attempt_id='', metadata_nofast=None):
        metadata_nofast = metadata_nofast or {}
        self.tx_contexts_lock.acquire()
        ctx = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        if not ctx:
            return

        ctx['repair_metadata_lock'].acquire()
        current_identity = (ctx['repair_epoch'], ctx['repair_mode'], ctx['attempt_id'])
        incoming_identity = (repair_epoch, repair_mode, attempt_id)
        if repair_epoch < ctx['repair_epoch']:
            ctx['repair_metadata_lock'].release()
            log_event('STALE_TRIGGER_REJECTED', workflow=self.workflow,
                      tx_id=transaction_id, function=self.function,
                      repair_mode=repair_mode, repair_epoch=repair_epoch,
                      attempt_id=attempt_id, state_before=current_identity,
                      state_after=current_identity)
            return False
        if (ctx['repair_mode'] is None or repair_epoch > ctx['repair_epoch'] or
                (ctx['repair_mode'] == OPT_REPAIR and repair_mode == PESSI_REPAIR)):
            ##print(f"repair mode changed from {self.repair_mode} to {repair_mode}")
            ctx['repair_mode'] = repair_mode
            self.last_transition_timestamp = time.time()
            ctx['repair_epoch'] = repair_epoch
            ctx['attempt_id'] = attempt_id
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
            log_event('REPAIR_METADATA_INSTALLED', workflow=self.workflow,
                      batch_id=ctx['batch_id'], tx_id=transaction_id,
                      function=self.function, repair_mode=repair_mode,
                      repair_epoch=repair_epoch, attempt_id=attempt_id,
                      state_before=current_identity, state_after=incoming_identity)
        ctx['repair_metadata_lock'].release()
        return ctx['repair_epoch'] == repair_epoch and ctx['repair_mode'] == repair_mode and ctx['attempt_id'] == attempt_id

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
            runnable = ctx['parent_executed'] == self.parent_cnt + ctx['subjection_waiting_cnt']
            log_event('RUNNABLE_CHECK', workflow=self.workflow,
                      batch_id=ctx['batch_id'], tx_id=transaction_id,
                      function=self.function, repair_mode=ctx['repair_mode'],
                      repair_epoch=ctx['repair_epoch'], attempt_id=ctx['attempt_id'],
                      state_before=ctx['parent_executed'], state_after=runnable,
                      workflow_parent_count=self.parent_cnt,
                      upstream_wait_count=ctx['subjection_waiting_cnt'])
            return runnable
        
    def trigger_next_function(self, transaction_id, ip, port ,dirty=False, batch_id="", repair_mode='', repair_epoch=1, attempt_id=''):
        url = f'http://{ip}:{port}/run'
        data = {
            'batch_id': batch_id,
            'transaction_id': transaction_id,
            'repair': True,
            'repair_mode':repair_mode
            ,'repair_epoch': repair_epoch
            ,'attempt_id': attempt_id
            }
        ##print(f"Triggering next function: {ip}:{port}, batch_id: {batch_id}, transaction_id: {transaction_id}, dirty: {dirty}", flush=True)
        requests.post(url, json=data)

    def fin_repair(self, batch_id, transaction_id, repair_mode, repair_epoch, attempt_id):
        url = f'http://{self.sink_addr}/fin_repair'
        ##print(f"Finishing repair: {self.function}, sending to sink: {url}, batch_id: {batch_id}, transaction_id: {transaction_id}", flush=True)
        data = { 'workflow_name':self.workflow  ,'batch_id': batch_id, "transaction_id": transaction_id, 'repair_mode':repair_mode,
                'repair_epoch': repair_epoch, 'attempt_id': attempt_id}
        requests.post(url, json=data)

    def abort_transaction(self, batch_id, transaction_id, repair_mode, repair_epoch, attempt_id, msg=""):
        ##print(f"abort transaction: {self.function}", flush=True)
        url = f'http://{self.sink_addr}/abort'
        data = {'batch_id': batch_id, "transaction_id": transaction_id, 'workflow_name': self.workflow, 'repair':True, 'repair_mode':repair_mode,
                'repair_epoch': repair_epoch, 'attempt_id': attempt_id, "error": msg}
        requests.post(url, json=data)

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
                        gevent.spawn(self.fin_repair, batch_id, transaction_id, repair_mode,
                                     ctx['repair_epoch'], ctx['attempt_id'])
                    )
                    break
                next_ip = self.function_pos[next_func]
                ##print(f"Trigger Next functions: {next_ip}:{self.successor_port}", flush=True)
                next_trigger_tasks.append(
                    gevent.spawn(
                    self.trigger_next_function,
                    transaction_id, next_ip, port, ctx['dirty'], batch_id, repair_mode,
                    ctx['repair_epoch'], ctx['attempt_id']
                    )
                )
        gevent.joinall(next_trigger_tasks)

    def run(self, transaction_id, is_repair, attempt_identity=None):
        # in first run, collect read/write set, and RYW subjection
        # in repair, use the metadata from redis.
        self.tx_contexts_lock.acquire()
        ctx_data = self.tx_contexts.get(transaction_id)
        self.tx_contexts_lock.release()
        if not ctx_data:
            return True, json.dumps({'Abort': True, 'error': "Transaction context not found"}), {}, {}, {}, 0, {}, False

        TxMetaData_thisFunc = {
                                "read_set": {}, 
                                "write_set": ctx_data['write_set'], 
                                "RYW_subjection": {},
                                "keys_from_RYW": ctx_data['keys_from_RYW'],
                                "keys_from_upstream": ctx_data['keys_from_upstream'],
                                "transaction_metadata": {},
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
            
            current_store.runtime_init(
                self.input, self.output, is_repair, ctx_data['repair_mode'],
                transaction_id, TxMetaData_thisFunc)
            local_ctx = {'workflow_name': self.workflow, 'function_name': self.function, 'store': current_store}
            # pre-exec
            # Serialize application code for the same transaction/function.
            # A mode promotion can install metadata while the old attempt is
            # running, but the new attempt waits so its shadow writes are last.
            ctx_data['execution_lock'].acquire()
            log_event('APPLICATION_EXEC_START', workflow=self.workflow,
                      batch_id=ctx_data['batch_id'], tx_id=transaction_id,
                      function=self.function, repair_mode=ctx_data['repair_mode'],
                      repair_epoch=ctx_data['repair_epoch'], attempt_id=ctx_data['attempt_id'],
                      state_before=ctx_data['container_state'], state_after=RUNNING)
            try:
                exec(self.code, local_ctx)
        # run function
                out = eval('main()', local_ctx)               
            except Exception as e:
                aborted = True
                error_traceback = traceback.format_exc()
                msg = json.dumps({'Abort': True, 'error': str(e), 'traceback': error_traceback})
                if 'INJECTED_DYNAMIC_ACCESS_ABORT' in str(e):
                    log_event('INJECTED_DYNAMIC_ACCESS_ABORT', workflow=self.workflow,
                              batch_id=ctx_data['batch_id'], tx_id=transaction_id,
                              function=self.function, repair_mode=ctx_data['repair_mode'],
                              repair_epoch=ctx_data['repair_epoch'], attempt_id=ctx_data['attempt_id'],
                              state_before=RUNNING, state_after=ABORTED)
            finally:
                ctx_data['execution_lock'].release()
                #print(f"Function {self.function} execution failed with traceback:\n{error_traceback}", flush=True)
        # the function finished repair, not abort, send data to waiting functions in fastpath..
        stale_result = False
        if is_repair and attempt_identity is not None:
            self.tx_contexts_lock.acquire()
            current_ctx = self.tx_contexts.get(transaction_id)
            current_identity = None if current_ctx is None else (
                current_ctx['repair_epoch'], current_ctx['repair_mode'], current_ctx['attempt_id'])
            self.tx_contexts_lock.release()
            stale_result = current_identity != attempt_identity
            if stale_result:
                log_event('STALE_RESULT_DROPPED', workflow=self.workflow,
                          batch_id=ctx_data['batch_id'], tx_id=transaction_id,
                          function=self.function, repair_mode=attempt_identity[1],
                          repair_epoch=attempt_identity[0], attempt_id=attempt_identity[2],
                          state_before=attempt_identity, state_after=current_identity)

        if is_repair and not stale_result:
            if self.fast_path_enabled:
                if aborted:
                    ctx_data['container_state'] = ABORTED
                    self.abort_transaction(ctx_data['batch_id'], transaction_id, ctx_data['repair_mode'],
                                           ctx_data['repair_epoch'], ctx_data['attempt_id'], msg)
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
        
        # Cleanup context if repair is done
        if is_repair:
            self.tx_contexts_lock.acquire()
            active_ctx = self.tx_contexts.get(transaction_id)
            active_identity = None if active_ctx is None else (
                active_ctx['repair_epoch'], active_ctx['repair_mode'], active_ctx['attempt_id'])
            cleanup_allowed = aborted or attempt_identity[1] == PESSI_REPAIR
            if active_ctx is not None and active_identity == attempt_identity and cleanup_allowed:
                del self.tx_contexts[transaction_id]
                log_event('CONTEXT_CLEANUP_ACCEPTED', workflow=self.workflow,
                          batch_id=ctx_data['batch_id'], tx_id=transaction_id,
                          function=self.function, repair_mode=attempt_identity[1],
                          repair_epoch=attempt_identity[0], attempt_id=attempt_identity[2],
                          state_before=active_identity, state_after='DELETED')
            else:
                log_event('CONTEXT_CLEANUP_REJECTED', workflow=self.workflow,
                          batch_id=ctx_data['batch_id'], tx_id=transaction_id,
                          function=self.function, repair_mode=attempt_identity[1],
                          repair_epoch=attempt_identity[0], attempt_id=attempt_identity[2],
                          state_before=attempt_identity, state_after=active_identity)
            self.tx_contexts_lock.release()

        return aborted, msg, TxMetaData_thisFunc["read_set"], TxMetaData_thisFunc["write_set"],TxMetaData_thisFunc["RYW_subjection"], io_latency, TxMetaData_thisFunc["transaction_metadata"], stale_result


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
    repair_states_input = inp.get('repair_states', {})
    repair_epoch = inp.get('repair_epoch', repair_states_input.get('_repair_epoch', 1 if is_repair else 0))
    attempt_id = inp.get('attempt_id', repair_states_input.get('_attempt_id', ''))
    batch_id = inp.get('batch_id', '')
    if is_repair:
        log_event('REPAIR_REQUEST_RECEIVED', workflow=runner.workflow,
                  batch_id=batch_id, tx_id=transaction_id,
                  function=runner.function, repair_mode=repair_mode,
                  repair_epoch=repair_epoch, attempt_id=attempt_id,
                  state_before='', state_after='RECEIVED')
    
    no_parent_execution = False

    if not is_repair:
        runner.save(transaction_id, inp.get('write_set', {}))
    else:
        if batch_id:
            ctx = runner.get_context(transaction_id)
            if ctx:
                ctx['batch_id'] = batch_id
            
            if runner.fast_path_enabled:
                no_parent_execution = inp.get('no_parent_execution', False)
            installed = runner.fetch_repair_metadata(transaction_id, repair_mode, repair_epoch,
                                                     attempt_id, repair_states_input)
            if not installed:
                return {'stale_result': True}
        else:
            ctx = runner.get_context(transaction_id)
            if ctx and ctx['repair_mode'] == PESSI_REPAIR:
                return json.dumps({'Abort': True, 'error': "PESSIMISTIC REPAIR should not be triggered without batch_id."})
            if ctx:
                # Cross-transaction optimistic wakeups address the reserved
                # container directly and historically omit batch metadata.
                batch_id = ctx['batch_id']
                repair_mode = ctx['repair_mode']
                repair_epoch = ctx['repair_epoch']
                attempt_id = ctx['attempt_id']

    if runner.check_runnable(transaction_id, is_repair, no_parent_execution, repair_mode):
        attempt_identity = (repair_epoch, repair_mode, attempt_id) if is_repair else None
        aborted, abort_msg, rs, ws, RYW_subjection, io_latency, transaction_metadata, stale_result = runner.run(transaction_id, is_repair, attempt_identity)
        if stale_result:
            return {'stale_result': True}
        if aborted:
            return abort_msg
        
        res = {
            "read_set": rs,
            "write_set": ws,
            "RYW_subjection": RYW_subjection,
            "io_latency": io_latency
            ,"transaction_metadata": transaction_metadata
        }
        proxy.status = 'ok'
        return res
    
    return ('OK', 200)

@proxy.route('/clear', methods=['POST'])
def clear():
    inp = request.get_json(force=True, silent=True) or {}
    transaction_id = inp.get('transaction_id', '')
    removed = runner.clear_context(transaction_id)
    return {'status': 'ok', 'removed': removed}

if __name__ == '__main__':
    server = WSGIServer(('0.0.0.0', 5000), proxy,
                        log=None, error_log=None)
    server.serve_forever()
