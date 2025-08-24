

import sys
from gevent import monkey
monkey.patch_all()
import time
import gevent.lock
from workersp_repo import Repository
from typing import Any, Dict, List
import requests
import re
import os
import logging

sys.path.append('../../config')
import config

sys.path.append('../function_manager')
from function_manager import FunctionManager


REPAIRED = 1
ABORTED = 2

log_file = '../../logging/workersp.log'

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
    def __init__(self, transaction_id: str, all_func: List[str], write_set, retry=False, term=0):
        self.transaction_id = transaction_id
        # {key: func}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx}
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.parent_executed: Dict[str, int] = {}
        self.retry = retry
        self.term = term
        self.valid = True
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
        self.repo = repo
        self.lock = gevent.lock.BoundedSemaphore() # guard self.states
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.func = self.repo.get_current_node_functions(self.host_addr, self.info_db)
        self.function_info = {}
        self.function_pos = {}
        for function_name in self.func:
            self.function_info[function_name] = self.repo.get_function_info(function_name, self.info_db)
            self.function_pos[function_name] = extract_ip(self.function_info[function_name]['ip'])
        
        self.node_list = node_list
        
        self.function_manager = FunctionManager(self.workflow_name, extract_ip(self.host_addr), function_info_addr, min_port, self.node_list, self.function_pos)
        # repairing batches and finished transactions
        min_port += 5000
        # config of different modes.


    # return the workflow state of the request
    def get_state(self, transaction_id, write_set, retry, term) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if transaction_id not in self.states or not self.states[transaction_id].valid:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, write_set, retry, term)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()  
            state.term = term
            state.retry = retry
            state.write_set.update(write_set)
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state

    # delete state
    def del_state(self, transaction_id: str, clear: bool):
        self.lock.acquire()
        if transaction_id in self.states:
            # logging.info('delete state of: %s', transaction_id)
            if clear:
                del self.states[transaction_id]
            else:
                self.states[transaction_id].lock.acquire()
                self.states[transaction_id].valid = False
                self.states[transaction_id].lock.release()
        self.lock.release()

    def commit_tx(self, transaction_id: str, write_set: Dict[str, int], term) -> None:
        log_message(f"[COMMIT] committing transaction {transaction_id}, write_set: {write_set}")
        commit_set = {}
        commit_jobs = []
        commit_start = time.time()
        for key, func in write_set.items():
            commit_set.setdefault(self.function_pos[func], set()).add(key)
        for ip, keys in commit_set.items():
            commiter_url = 'http://{}:7500/commit'.format(ip)
            data = {
                'commit_keys': list(keys),
            }
            commit_jobs.append(gevent.spawn(requests.post, commiter_url, json=data))
        gevent.joinall(commit_jobs)
        self.clear_access_log_on_worker(transaction_id)
        commit_end = time.time()
        self.notify_gateway(transaction_id, term, commit_end - commit_start, False, '')


    def abort_tx(self, transaction_id, term, Abort_type):
        # trigger next run of the transaction under pessimistic repair mode
        # logging.info(f"[ABORT] aborting transaction {transaction_id}")
        self.notify_gateway(transaction_id, term, 0, True, Abort_type)

    def clear_access_log_on_worker(self, transaction_id: str) -> None:
        clear_jobs = []
        log_message(f"[CLEAR ACCESS LOG] clear access log of tx {transaction_id} on all workers {self.node_list}")
        for ip in self.node_list:
            clear_url = 'http://{}:6000/clear_state'.format(ip)
            data = {'transaction_id': transaction_id, 'workflow_name': self.workflow_name, 'commit':True}
            clear_jobs.append(gevent.spawn(requests.post, clear_url, json=data))
        gevent.joinall(clear_jobs)

    def notify_gateway(self, transaction_id, term, commit_latency=0, abort=False, Abort_type=''):
        notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
        payload = {
            'transaction_id': transaction_id,
            'commit_latency': commit_latency,
            'abort': abort,
            'Abort_type': Abort_type,
            'term':term
        }
        requests.post(notify_url, json=payload)

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            self.commit_tx(state.transaction_id, state.write_set, state.term)
            return
        func_info = self.function_info[function_name]
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)


    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str,  no_parent_execution = False) -> None:
        state.lock.acquire()
        if not state.valid:
            state.lock.release()
            return
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        # remember to release state.lock
        if runnable:
            state.executed[function_name] = True
            state.lock.release()
            self.run_function(state, function_name)
        else:
            state.lock.release()

    # trigger a function that runs on remote machine
    def trigger_function_remote(self, state: TransactionState, function_name: str, remote_addr: str, no_parent_execution = False) -> None:
        # logging.info(f'trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            # basic infomation
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            # collected for validation. updated only in first run.
            'write_set': state.write_set,
            'term': state.term
        }
        requests.post(remote_url, json=data)

    # check if a function's parents are all finished
    # If in repair mode, add upstream parents 
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.function_info[function_name]
            # parent count: the sum of parents in workflow graph and parents in subject table
        return state.parent_executed[function_name] == info['parent_cnt'] and not state.executed[function_name] 

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        # if function in repair mode and not dirty, skip running
        info = self.function_info[function_name]
        # if function in repair mode and not dirty, skip running
        successful, Abort_type = self.run_normal(state, info)
        if not successful:
            self.abort_tx(state.transaction_id, state.term, Abort_type)
            return

        # clear parent cnt and run state. For repairing. Remove the repair state of this function.
        # trigger downstream functions, including the ones in write relation table.
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]    
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        name = info['function_name']
        res = self.function_manager.run(name, state.transaction_id, state.write_set, state.term)
        end = time.time()
        log_message(f"running function {name}, transaction_id: {state.transaction_id}, write_set: {state.write_set}, port:{res['port']}")
        if res.get("Abort", False):
            log_message(f"function {name} trigger abort: {res['error']}")
            return False, res['Abort_type']

        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.
        if not state.valid:
            state.lock.release()
            return None, None
        state.write_set.update(res["write_set"])
        state.lock.release()
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start, 'term':state.term})
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency'], 'term':state.term}) 
        # logging.info(f"function {info['function_name']} done, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")
        return True, ''

    def clear_mem(self, transaction_id):
        self.repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)