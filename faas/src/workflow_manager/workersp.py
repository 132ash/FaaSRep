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

sys.path.append('../../config')
import config

sys.path.append('../function_manager')
from function_manager import FunctionManager


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
    def __init__(self, transaction_id: str, all_func: List[str], write_set, lock_set):
        self.transaction_id = transaction_id
        # {func: {key: version}}
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.parent_executed: Dict[str, int] = {}
        self.stop_running = False # used to stop running the transaction, e.g., when the function is aborted.
        # used only in remote lock mode.
        self.lock_set:Dict = lock_set

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
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.func = self.repo.get_current_node_functions(self.host_addr, self.info_db)
        self.function_info = {}
        self.function_pos = {}
        for function_name in self.func:
            self.function_info[function_name] = self.repo.get_function_info(function_name, self.info_db)
            self.function_pos[function_name] = self.function_info[function_name]['ip']
        self.repo = repo

        self.node_list = node_list
        
        self.function_manager = FunctionManager(function_info_addr,  min_port)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000
        self.GATEWAY_ADDR = config.GATEWAY_ADDR
        # config of different modes.

    # return the workflow state of the request
    def get_state(self, retry_after_abort, transaction_id: str, write_set, lock_set) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if transaction_id not in self.states or retry_after_abort:
            self.states[transaction_id] = TransactionState(transaction_id, self.func,  write_set,  lock_set)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            state.lock_set.update(lock_set)
            state.write_set.update(write_set)
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state

    # delete state
    def del_state(self, transaction_id: str):
        self.lock.acquire()
        if transaction_id in self.states:
            logging.info('delete state of: %s', transaction_id)
            del self.states[transaction_id]
        self.lock.release()

    def stop_transaction(self, transaction_id: str) -> None:
        self.lock.acquire()
        if transaction_id not in self.states:
            return
        state = self.states[transaction_id]
        state.lock.acquire()
        state.stop_running = True
        state.lock.release()

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            self.repo.beldi_commit(state.transaction_id, state.lock_set)
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
        if state.stop_running:
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
        logging.info(f'trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            'write_set': state.write_set,
            'lock_set': state.lock_set

        }
        requests.post(remote_url, json=data)

    def abort_tx(self, transaction_id):
        # trigger next run of the transaction under pessimistic repair mode
        notify_url = "http://{}/notify".format(self.GATEWAY_ADDR)
        payload = {
            'transaction_id_list': [[transaction_id]],
            'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
            'abort': True
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
        successful, lock_set = self.run_normal(state, info)
        if not successful:
            self.repo.release_lock(state.transaction_id, lock_set)
            self.abort_tx(state.transaction_id)
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
        logging.info(f"running function {name}, transaction_id: {state.transaction_id}, write_set: {state.write_set}")
        res = self.function_manager.run(name, state.transaction_id, state.write_set, state.lock_set)
        end = time.time()
        if res.get("Abort", False):
            logging.error(f"function {name} trigger abort: {res['error']}")
            return False, res['lock_set']
            
        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.

        state.write_set.update(res["write_set"])
        state.lock_set.update(res['lock_set'])
        state.lock.release()
        self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'lock', 'time': res['lock_latency']})
        logging.info(f"function {info['function_name']} done, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")

        return True, res['lock_set']

    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)