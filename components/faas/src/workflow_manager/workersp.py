import sys
import logging
import time
import gevent
import gevent.lock
import workersp_repo
from typing import Any, Dict, List
import requests
import re

sys.path.append('../../config')
import config

sys.path.append('../function_manager')
from function_manager import FunctionManager

repo = workersp_repo.Repository()

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

class TransactionState:
    def __init__(self, transaction_id: str, all_func: List[str], function_pos: Dict[str, str]={}):
        self.transaction_id = transaction_id
        # {func: {key: version}}
        self.read_set:Dict[str:Dict[str:str]] = {}
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = {}
        # {func: []]}
        self.expired_keys = {}
        self.repair = False # if the transaction is in repair mode
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.function_pos: Dict[str, str] = function_pos
        self.parent_executed: Dict[str, int] = {}
        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str):
        global min_port

        self.lock = gevent.lock.BoundedSemaphore() # guard self.states
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.function_info: Dict[str, dict] = {}

        self.info_db = workflow_name + '_function_info'
        self.common_db = 'common'
        self.meta_db = workflow_name + '_workflow_metadata'

        self.func = repo.get_current_node_functions(self.host_addr, self.info_db)
        self.node_list = repo.get_all_addrs(self.common_db)
        
        self.function_manager = FunctionManager(function_info_addr, min_port, self.node_list)
        min_port += 5000

    # return the workflow state of the request
    def get_state(self, transaction_id: str, function_pos, read_set, write_set,  repair, expired_keys) -> TransactionState:
        self.lock.acquire()
        if transaction_id not in self.states:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, function_pos, read_set, write_set, repair, expired_keys)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            state.repair = repair
            state.expired_keys = expired_keys
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state
    
    def del_state_remote(self, transaction_id: str, remote_addr: str):
        url = 'http://{}/clear'.format(remote_addr)
        requests.post(url, json={'transaction_id': transaction_id, 'workflow_name': self.workflow_name})

    # delete state
    def del_state(self, transaction_id: str, master: bool):
        logging.info('delete state of: %s', transaction_id)
        self.lock.acquire()
        if transaction_id in self.states:
            del self.states[transaction_id]
        self.lock.release()
        if master:
            jobs = []
            addrs = repo.get_all_addrs(self.meta_db)
            for addr in addrs:
                if addr != self.host_addr:
                    jobs.append(gevent.spawn(self.del_state_remote, transaction_id, addr))
            gevent.joinall(jobs)

    # get function's info from database
    # the result is cached
    def get_function_info(self, function_name: str) -> Any:
        if function_name not in self.function_info:
            self.function_info[function_name] = repo.get_function_info(function_name, self.info_db)
        return self.function_info[function_name]
    
    def validate_tx(self, workflow_name, transaction_id, read_set, write_set, function_pos):
        remote_url = 'http://{}/validate'.format(config.GATEWAY_ADDR)
        requests.post(remote_url, json={'workflow_name': workflow_name, 'transaction_id': transaction_id, 'read_set': read_set, 'write_set': write_set, "function_pos": function_pos})
      

    def commit_tx(self, transaction_id):
        repo.save_tx_result(transaction_id, self.meta_db)
        remote_url = 'http://{}/fin_repair'.format(config.GATEWAY_ADDR)
        requests.post(remote_url, json={'transaction_id': transaction_id})


    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if state.repair:
                self.commit_tx(state.transaction_id)
            else:
                function_pos = state.function_pos
                self.validate_tx(self.workflow_name, state.transaction_id, state.read_set, state.write_set, function_pos)
            return
        func_info = self.get_function_info(function_name)
        print(f"trigger func {function_name} with ip {func_info['ip']}")
        if func_info['ip'] == self.host_addr:
            # function runs on local
            # update cache if in repair mode and this node has expired_keys.
            if state.repair and self.host_addr in state.expired_keys:
                repo.update_cache(state.transaction_id, state.expired_keys[self.host_addr])
                state.expired_keys.pop(self.host_addr)
            self.trigger_function_local(state, function_name, func_info['ip'], no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str, ip:str, no_parent_execution = False) -> None:
        logging.info('trigger local function: %s of: %s', function_name, state.transaction_id)
        state.lock.acquire()
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        # remember to release state.lock
        if runnable:
            state.executed[function_name] = True
            state.function_pos[function_name] = extract_ip(ip)
            state.lock.release()
            self.run_function(state, function_name)
        else:
            state.lock.release()

    # trigger a function that runs on remote machine
    def trigger_function_remote(self, state: TransactionState, function_name: str, remote_addr: str, no_parent_execution = False) -> None:
        logging.info('trigger remote function: %s on: %s of: %s', function_name, remote_addr, state.transaction_id)
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            'function_pos':state.function_pos, 
            'read_set': state.read_set,
            'write_set': state.write_set
        }
        response = requests.post(remote_url, json=data)
        response.close()

    # check if a function's parents are all finished
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.get_function_info(function_name)
        return state.parent_executed[function_name] == info['parent_cnt'] and not state.executed[function_name]

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        logging.info('run function: %s of: %s', function_name, state.transaction_id)
        # end functions
        
        info = self.get_function_info(function_name)
        self.run_normal(state, info)
        
        # trigger next functions
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        res = self.function_manager.run(state.function_pos, info['function_name'], state.transaction_id,
                             info['input'], info['output'], state.write_set)
        end = time.time()

        state.lock.acquire()
        state.read_set[info["function_name"]] = res["read_set"]
        state.write_set = res["write_set"]
        state.lock.release()

        repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'all', 'time': end - start})

    def clear_mem(self, transaction_id):
        repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        repo.clear_db(transaction_id)