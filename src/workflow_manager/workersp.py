import logging
from gevent import monkey
monkey.patch_all()
import sys
import os
import time
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import gevent.lock
from workersp_repo import Repository
from typing import Any, Dict, List
import requests
import re
from src.function_manager.function_manager import FunctionManager
import config

LOG_DIR = ROOT_DIR / 'logging'
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / 'workersp.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

def setup_logger():
    logger = logging.getLogger('workersp')
    logger.setLevel(logging.INFO)
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# 全局logger实例
logger = setup_logger()

def log_message(message):
    logger.info(message)
    for handler in logger.handlers:
        handler.flush()

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

class TransactionState:
    def __init__(self, transaction_id: str, all_func: List[str], read_set, write_set, term=0, birth_seq=None):
        self.transaction_id = transaction_id
        self.term = int(term)
        self.birth_seq = birth_seq
        # {func: {key: version}} 
        self.read_set:Dict[str:Dict[str:str]] = read_set
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx}
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.parent_executed: Dict[str, int] = {}
        self.valid = True
        self.metrics = {}
        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0
        

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str, repo:Repository, node_list:list) -> None:
        global min_port

        self.info_db = workflow_name + '_function_info'
        self.common_db = 'common'
        self.meta_db = workflow_name + '_workflow_metadata'

        self.lock = gevent.lock.BoundedSemaphore() # guard self.states
        self.host_addr = host_addr
        self.repo = repo
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.func = self.repo.get_current_node_functions(self.host_addr, self.info_db)
        self.function_info = {}
        self.function_pos = {}
        for function_name in self.func:
            self.function_info[function_name] = self.repo.get_function_info(function_name, self.info_db)
            self.function_pos[function_name] = extract_ip(self.function_info[function_name]['ip'])
        self.repo = repo
        self.transaction_sink_addr = self.function_pos[self.repo.get_end_function(self.meta_db)] + ':6000'

        self.node_list = node_list
        self.function_manager = FunctionManager(extract_ip(self.host_addr), self.workflow_name, function_info_addr, min_port, self.node_list, self.function_pos)
        # repairing batches and finished transactions
        min_port += 5000

    # return the workflow state of the request
    def get_state(self, transaction_id: str, read_set, write_set, term=0, birth_seq=None) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if (transaction_id not in self.states or not self.states[transaction_id].valid
                or self.states[transaction_id].term != int(term)):
            self.states[transaction_id] = TransactionState(transaction_id, self.func, read_set, write_set, term, birth_seq)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            state.read_set.update(read_set)
            state.write_set.update(write_set)
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state

    def clear_containers(self):
        self.function_manager.clear_containers()

    # delete state
    def del_state(self, transaction_id: str, fin):
        self.lock.acquire()
        if transaction_id in self.states:
            if fin:
                del self.states[transaction_id]
            else:
                state = self.states[transaction_id]
                state.lock.acquire()
                state.valid = False
                state.lock.release()
        self.lock.release()
    
    def validate_tx(self, transaction_id, read_set, write_set) -> None:
        url = 'http://{}/validate'.format(self.transaction_sink_addr)
        data = {
            'transaction_id': transaction_id,
            'workflow_name': self.workflow_name,
            'read_set': read_set,
            'write_set': write_set
        }
        requests.post(url, json=data)

    def active_abort_tx(self,transaction_id):
        url = 'http://{}/notify'.format(config.GATEWAY_ADDR)
        data = {"aborted_txs":[transaction_id], 'timestamps':[0,0]}
        requests.post(url, json=data)

    @staticmethod
    def _boki_post(address, path, data, timeout=35):
        response = requests.post(f'http://{address}{path}', json=data, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _boki_notify(self, state, status, abort_type=None, metrics=None, error=None):
        data = {'txid': state.transaction_id, 'term': state.term, 'status': status,
                'abort_type': abort_type, 'metrics': metrics or {}, 'error': error}
        try:
            self._boki_post(config.GATEWAY_ADDR, '/notify', data)
        except Exception:
            logging.exception('failed to notify gateway for %s term %s', state.transaction_id, state.term)

    def boki_abort(self, state, abort_type, error=''):
        """Discard is deliberately confirmed before the strict-2PL release."""
        state.lock.acquire()
        if not state.valid:
            state.lock.release()
            return
        state.valid = False
        state.lock.release()
        try:
            discarded = self._boki_post(config.SHADOW_SERVICE_ADDR, '/discard', {
                'txid': state.transaction_id, 'term': state.term, 'reason': abort_type})
            if discarded.get('status') != 'DISCARDED':
                self._boki_notify(state, 'error', 'ERROR', error=discarded.get('error', 'discard failed'))
                return
            released = self._boki_post(config.LOCK_MANAGER_ADDR, '/abort', {
                'txid': state.transaction_id, 'term': state.term, 'abort_type': abort_type})
            metrics = dict(released.get('metrics', {}))
            metrics.update(state.metrics)
            self._boki_notify(state, 'aborted', abort_type, metrics, error)
        except Exception as exc:
            # Do not unlock when discard is not known to have completed.
            self._boki_notify(state, 'error', 'ERROR', error=str(exc))

    def boki_commit(self, state):
        state.lock.acquire()
        if not state.valid:
            state.lock.release()
            return
        state.valid = False
        state.lock.release()
        flush_id = f'{state.transaction_id}:{state.term}:flush'
        try:
            flushed = None
            for _ in range(3):
                flushed = self._boki_post(config.SHADOW_SERVICE_ADDR, '/flush', {
                    'txid': state.transaction_id, 'term': state.term, 'flush_id': flush_id})
                if flushed.get('status') == 'FLUSHED':
                    break
                if flushed.get('status') != 'RETRY':
                    self._boki_notify(state, 'error', 'ERROR', error=flushed.get('error', 'flush failed'))
                    return
                gevent.sleep(getattr(config, 'SHADOW_FLUSH_RETRY_SECONDS', 0.05))
            if not flushed or flushed.get('status') != 'FLUSHED':
                self._boki_notify(state, 'error', 'ERROR', error='flush retry budget exhausted; locks retained')
                return
            unlocked = self._boki_post(config.LOCK_MANAGER_ADDR, '/unlock', {
                'txid': state.transaction_id, 'term': state.term, 'all': True})
            if unlocked.get('status') != 'RELEASED':
                self._boki_notify(state, 'error', 'ERROR', error=unlocked.get('error', 'unlock failed'))
                return
            # Completion cleanup is non-critical after the release confirmation.
            try:
                self._boki_post(config.SHADOW_SERVICE_ADDR, '/complete', {'txid': state.transaction_id, 'term': state.term})
            except Exception:
                logging.exception('shadow complete cleanup failed for %s', state.transaction_id)
            metrics = dict(unlocked.get('metrics', {}))
            metrics.update({'flush_latency': flushed.get('flush_latency', 0),
                            'flushed_key_count': flushed.get('flushed_key_count', 0)})
            metrics.update(state.metrics)
            self._boki_notify(state, 'committed', metrics=metrics)
        except Exception as exc:
            self._boki_notify(state, 'error', 'ERROR', error=str(exc))

    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if config.SYSTEM_MODE == 'BOKI_SN':
                self.boki_commit(state)
                return
            self.validate_tx(state.transaction_id, state.read_set, state.write_set)
            return
        func_info = self.function_info[function_name]
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str,  no_parent_execution = False) -> None:
        ##log_message(f'trigger local function: {function_name} of: {state.transaction_id}, repair:{state.repair}, repair_mode:{state.repair_mode}, repair_mode_changed:{state.repair_mode_changed[function_name]}')
        state.lock.acquire()
        if not state.valid:
            state.lock.release()
            return
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        # remember to release state.lock
        if runnable:
            state.lock.release()
            self.run_function(state, function_name)
        else:
            state.lock.release()

    # trigger a function that runs on remote machine
    def trigger_function_remote(self, state: TransactionState, function_name: str, remote_addr: str, no_parent_execution = False) -> None:
        ##log_message(f'trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            # basic infomation
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            'term': state.term,
            'birth_seq': state.birth_seq,
            # collected for validation. updated only in first run. 
            'read_set': state.read_set,
            'write_set': state.write_set
        }
        requests.post(remote_url, json=data)
    
    # check if a function's parents are all finished
    # If in repair mode, add upstream parents 
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.function_info[function_name]
        return state.parent_executed[function_name] == info['parent_cnt']

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        # if function in repair mode and not dirty, skip running
        info = self.function_info[function_name]
        crosstx_jobs = []
        # if function in repair mode and not dirty, skip running
        outcome = self.run_normal(state, info)
        if outcome is not True:
            if config.SYSTEM_MODE == 'BOKI_SN':
                abort_type = outcome.get('abort_type', 'ERROR') if isinstance(outcome, dict) else 'ERROR'
                self.boki_abort(state, abort_type, outcome.get('error', '') if isinstance(outcome, dict) else '')
            else:
                self.active_abort_tx(state.transaction_id)
            return
        state.lock.acquire()
        state.parent_executed[function_name] = 0
        state.lock.release()
        # trigger downstream functions, including the ones in write relation table.
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]    
        jobs.extend(crosstx_jobs)
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        name = info['function_name']
        ##log_message(f"Running function {name} for transaction {state.transaction_id}, repair: {state.repair}, repair_mode: {state.repair_mode}, REPAIR_STATES: {state.repair_states.get(name, {})}")
        res = self.function_manager.run(name, state.transaction_id, state.write_set, state.term, state.birth_seq)
        end = time.time()
        if res.get("Abort", False):
            logging.error(f"function {name} trigger abort: {res['error']}")
            return res
        state.lock.acquire()
        if not state.valid:
            state.lock.release()
            return None
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.
        state.write_set.update(res["write_set"])
        if config.SYSTEM_MODE == 'BOKI_SN':
            for key, value in res.get('metrics', {}).items():
                state.metrics[key] = state.metrics.get(key, 0) + value
        # ##log_message(f"Function {name} executed in {end - start:.2f}s, IO latency: {res['io_latency']:.2f}s saved.")
        state.read_set[info["function_name"]] = res["read_set"]
        state.lock.release()
        if getattr(config, 'COLLECT_FUNCTION_LATENCY', False):
            self.repo.save_latencies([
                {'workflow_name': self.workflow_name, 'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start},
                {'workflow_name': self.workflow_name, 'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']}
            ])
        return True

    def clear_mem(self, transaction_id):
        self.repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)
