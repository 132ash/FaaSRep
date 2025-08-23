import logging
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

sys.path.append('../../config')
import config

sys.path.append('../function_manager')
from function_manager import FunctionManager
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

REPAIRED = 1
ABORTED = 2

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


class TransactionState:
    def __init__(self, create_timestamp, transaction_id: str, all_func: List[str], write_set, term):
        self.transaction_id = transaction_id
        # {func: {key: version}}
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.create_timestamp = create_timestamp
        self.parent_executed: Dict[str, int] = {}
        self.term = term 
        self.valid = True
        # used only in remote lock mode.

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
        
        self.function_manager = FunctionManager(extract_ip(self.host_addr), self.function_pos, workflow_name, function_info_addr,  min_port)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000
        self.GATEWAY_ADDR = config.GATEWAY_ADDR
        # config of different modes.

    # return the workflow state of the request
    def get_state(self, create_timestamp, transaction_id: str, write_set, term) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if transaction_id not in self.states or not self.states[transaction_id].valid:
            self.states[transaction_id] = TransactionState(create_timestamp, transaction_id, self.func,  write_set, term)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            state.write_set.update(write_set)
            state.term = term
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state

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

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            commit_start = time.time()
            self.repo.beldi_commit(state.transaction_id)
            commit_end = time.time()
            self.abort_or_commit_tx(state.transaction_id, False, state.term, '', commit_end-commit_start)
            #log_message(f"Transaction {state.transaction_id} committed.")
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
        #log_message(f'txid {state.transaction_id} trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            'write_set': state.write_set,
            'create_timestamp': state.create_timestamp,
            'term':state.term

        }
        requests.post(remote_url, json=data)

    def abort_or_commit_tx(self, transaction_id, aborted, term, Abort_type, commit_time):
        # trigger next run of the transaction under pessimistic repair mode
        if aborted:
            data = {'workflow_name': self.workflow_name, 'transaction_id': transaction_id}
            clear_jobs = []
            for worker_node in self.node_list:
                url = f"http://{worker_node}:{config.WORKERSP_PORT}/clear"
                clear_jobs.append(gevent.spawn(requests.post, url, json=data))
            gevent.joinall(clear_jobs)

        notify_url = "http://{}/notify".format(self.GATEWAY_ADDR)
        payload = {
            'transaction_id': transaction_id,
            'abort': aborted,
            'Abort_type':Abort_type,
            'term':term,
            'commit_latency':commit_time
        }
        requests.post(notify_url, json=payload)

    
    # check if a function's parents are all finished
    # If in repair mode, add upstream parents 
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.function_info[function_name]
        return state.parent_executed[function_name] == info['parent_cnt'] and not state.executed[function_name] 

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        # if function in repair mode and not dirty, skip running
        info = self.function_info[function_name]
        successful, Abort_type = self.run_normal(state, info)
        if successful is None:
            return
        if not successful:
           # logging.error(f"function {function_name} failed to run")
            #log_message(f"function {function_name} in {state.transaction_id} failed to run, trigger abort.")
            self.abort_or_commit_tx(state.transaction_id, True, state.term, Abort_type, 0)
            return
        # trigger downstream functions, including the ones in write relation table.
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]    
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        name = info['function_name']
        #log_message(f"running function {name}, transaction_id: {state.transaction_id}, write_set: {state.write_set}")
        res = self.function_manager.run(state.create_timestamp, name, state.transaction_id, state.write_set, state.term)
        end = time.time()
        #log_message(f"function {name} in {state.transaction_id} done, res:{res}")
        if res.get("Abort", False):
           # logging.error(f"txid {state.transaction_id} function {name} trigger abort: {res['error']}")
            if res['Abort_type'] == 'ERROR':
                log_message(f"Function {name} in {state.transaction_id} failed with error: {res['error']}")
            #log_message(f"Function {name} in {state.transaction_id} aborted: {res['error']}, Abort_type:{res['Abort_type']}")
            return False, res['Abort_type']
            
        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.
        if not state.valid:
            state.lock.release()
            return None, None
        state.write_set.update(res["write_set"])
        state.lock.release()
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'lock', 'time': res['lock_latency'], 'term':state.term})
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start, 'term':state.term})
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency'], 'term':state.term}) 
        ## logging.info(f"function {info['function_name']} done, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")

        return True,  ''

    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)