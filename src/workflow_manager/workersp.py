import logging
from gevent import monkey
monkey.patch_all()
import sys
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

RUNNING = config.RUNNING
ABORTED = config.ABORTED
REPAIRED = config.REPAIRED

OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


class ReservePool:
    def __init__(self):
        self.pool = {}  # {transaction_id: {lock: xx, containers:[container1, container2, ...]}}
        self.queue_lock = gevent.lock.BoundedSemaphore()
        
    def reserve(self, transaction_id, container):
        self.queue_lock.acquire()
        if transaction_id not in self.pool:
            self.pool[transaction_id] = {"lock": gevent.lock.BoundedSemaphore(), "containers": []}
        self.queue_lock.release()
        self.pool[transaction_id]["lock"].acquire()
        self.pool[transaction_id]["containers"].append(container)
        self.pool[transaction_id]["lock"].release()

    def release(self, fin_tx_list):
        for transaction_id in fin_tx_list:
            if transaction_id in self.pool:
                for container in self.pool[transaction_id]["containers"]:
                    container.return_to_pool()
                self.pool.pop(transaction_id, None)

class TransactionState:
    def __init__(self, transaction_id: str, all_func: List[str], container_port, read_set, write_set, batch_id, RYW_subjection, repair,repair_mode, repair_states):
        self.transaction_id = transaction_id
        # {func: {key: version}} 
        self.read_set:Dict[str:Dict[str:str]] = read_set
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx}
        self.RYW_subjection:Dict = RYW_subjection
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.container_port: Dict[str, str] = container_port
        self.parent_executed: Dict[str, int] = {}
        self.stop_running = False # used to stop running the transaction, e.g., when the function is aborted.

        # repair state, used only when fast-path is turned off.
        self.repair = repair
        self.repair_mode = repair_mode
        self.repair_subjection_upcnt = {}  # {func: up_cnt}
        self.repair_states = repair_states 
        self.batch_id = batch_id
        self.repair_mode_changed = False


        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0
        

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str, reserve_pool:ReservePool, repo:Repository, node_list:list) -> None:
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
        self.reserve_pool:ReservePool = reserve_pool
        for function_name in self.func:
            self.function_info[function_name] = self.repo.get_function_info(function_name, self.info_db)
            self.function_pos[function_name] = extract_ip(self.function_info[function_name]['ip'])
        self.repo = repo
        self.transaction_sink_addr = self.function_pos[self.repo.get_end_function(self.meta_db)] + ':6000'

        self.node_list = node_list
        
        self.function_manager = FunctionManager(extract_ip(self.host_addr), self.workflow_name, function_info_addr, self.transaction_sink_addr, min_port, self.node_list, reserve_pool, self.function_pos)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000
        # config of different modes.
        self.FAST_PATH = config.FAST_PATH
        self.OPTIMISTIC_REPAIR = config.OPTIMISTIC_REPAIR

    # return the workflow state of the request
    def get_state(self, retry_after_abort, transaction_id: str, container_port, read_set, write_set, batch_id, RYW_subjection,  repair, repair_mode, repair_states) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if transaction_id not in self.states or retry_after_abort:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, container_port, read_set, write_set, batch_id, RYW_subjection, repair,repair_mode, repair_states)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            if repair:
                state.repair = repair
                # new repair mode appears, the execution state of functions should be reset.
                if repair_mode != state.repair_mode:
                    state.repair_mode_changed = True
                state.repair_mode = repair_mode
                state.repair_states = repair_states 
                state.batch_id = batch_id
            else:
                state.container_port.update(container_port)
                state.read_set.update(read_set)
                state.write_set.update(write_set)
                state.RYW_subjection.update(RYW_subjection)
            state.lock.release()
        state = self.states[transaction_id]
        self.lock.release()
        return state

    # delete state
    def del_state(self, transaction_id: str):
        self.lock.acquire()
        if transaction_id in self.states:
            #logging.info('delete state of: %s', transaction_id)
            del self.states[transaction_id]
        self.lock.release()
    
    def validate_tx(self, transaction_id, read_set, write_set, container_port, RYW_subjection) -> None:
        url = 'http://{}/validate'.format(self.transaction_sink_addr)
        data = {
            'transaction_id': transaction_id,
            'workflow_name': self.workflow_name,
            'read_set': read_set,
            'write_set': write_set,
            'container_port': container_port,
            'RYW_subjection': RYW_subjection
        }
        requests.post(url, json=data)

    def tx_aborted_or_repaired(self,transaction_id, state,is_repair, batch_id='', repair_mode=''):
        if state == ABORTED:
            url = 'http://{}/abort'.format(self.transaction_sink_addr)
            #logging.info(f"Transaction {transaction_id} in batch {batch_id} is aborted.")
        else:
            #logging.info(f"Transaction {transaction_id} in batch {batch_id} is repaired.")
            url = 'http://{}/fin_repair'.format(self.transaction_sink_addr)
        # trigger next run of the transaction under pessimistic repair mode
        data = {'batch_id':batch_id, 'transaction_id': transaction_id, 'workflow_name': self.workflow_name, 'repair_mode':repair_mode, 'repair': is_repair}
        requests.post(url, json=data)

    def trigger_repair(self, batch_id, transaction_id, function_name, no_parent_execution, port, repair_mode):
        base_url = 'http://127.0.0.1:{}/{}'
        data = {'batch_id':batch_id, 'transaction_id': transaction_id, "no_parent_execution": no_parent_execution, 'repair': True, 'repair_mode': repair_mode}
         # try:
        requests.post(base_url.format(port, 'run'), json=data)
        # except:
        #     state = self.states[transaction_id]
        #     self.function_manager.run(function_name, transaction_id, state.write_set, True, batch_id)

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if not state.repair:
                self.validate_tx(state.transaction_id, state.read_set, state.write_set, state.container_port, state.RYW_subjection)
            else:
                self.tx_aborted_or_repaired(state.transaction_id, REPAIRED, state.repair, state.batch_id, state.repair_mode)
            return
        func_info = self.function_info[function_name]
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    def crosstx_trigger_function(self, transaction_id: str, function_name: str) -> None:
        if transaction_id not in self.states or self.states[transaction_id].repair_mode != OPT_REPAIR:
            #logging.info(f"[CROSS TRIGGER] Transaction {transaction_id} doesn't need the data from {function_name}")
            return
        state = self.states[transaction_id]
        state.lock.acquire()
        state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        if runnable:
            state.executed[function_name] = True
            state.lock.release()
            self.run_function(state, function_name)
        else:
            state.lock.release()

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str,  no_parent_execution = False) -> None:
        #logging.info(f'trigger local function: {function_name} of: {state.transaction_id}')
        state.lock.acquire()
        if state.repair and state.repair_mode_changed:
            upstream_keys = state.repair_states[function_name]["upstream_keys"]
            upstream_fetch_info = self.repo.subjection_collector.fetch_upstream_keys(upstream_keys, state.transaction_id, function_name, self.function_pos) 
            upstream_waiting_count = self.repo.subjection_collector.prepair_subjection_before_repair(state.transaction_id, function_name, state.repair_states[function_name]["upstream_keys"],upstream_fetch_info)  
            #logging.info(f"[REPAIR FETCH UPSTREAM] upstream_keys:{upstream_keys}, upstream waiting count: {upstream_waiting_count}")
            state.repair_subjection_upcnt[function_name] = upstream_waiting_count
            state.parent_executed[function_name] = 0
            state.executed[function_name] = False
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
        #logging.info(f'trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            # basic infomation
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            # collected for validation. updated only in first run.
            'container_port':state.container_port, 
            'read_set': state.read_set,
            'write_set': state.write_set,
            'RYW_subjection':state.RYW_subjection,
            # get from validator. repair metadata.
            'batch_id': state.batch_id,
            'repair': state.repair,
            'repair_mode': state.repair_mode,
            'repair_states': state.repair_states
        }
        requests.post(remote_url, json=data)

    def trigger_function_cross_tx(self, func_info):
        downstream_tx_id, function_name, remote_addr = func_info[0], func_info[1], func_info[2]
        remote_url = 'http://{}:7500/crosstx_req'.format(remote_addr)
        data = {
            'transaction_id': downstream_tx_id,
            'function_name': function_name,
            'workflow_name': self.workflow_name
        }
        requests.post(remote_url, json=data)
    
    # check if a function's parents are all finished
    # If in repair mode, add upstream parents 
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.function_info[function_name]
        up_cnt = 0
        if state.repair:
            up_cnt = state.repair_subjection_upcnt.get(function_name, 0)
            # parent count: the sum of parents in workflow graph and parents in subject table
        return state.parent_executed[function_name] == info['parent_cnt'] + up_cnt and not state.executed[function_name] 

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        # if function in repair mode and not dirty, skip running
        repair_metadata = state.repair_states.get(function_name, {})
        dirty = repair_metadata.get("dirty", False)
        info = self.function_info[function_name]
        crosstx_jobs = []
        # if function in repair mode and not dirty, skip running
        if not state.repair or dirty:
            successful = self.run_normal(state, info)
            if not successful:
                self.tx_aborted_or_repaired(state.transaction_id,ABORTED,state.repair, state.batch_id, state.repair_mode)
                return
            
        if self.OPTIMISTIC_REPAIR:
            if state.repair:
                downstream_funcs = self.repo.subjection_collector.set_state_and_get_waiting_downstream(state.transaction_id, function_name, REPAIRED)
                #logging.info(f"trigger downstream_funcs waiting function: {downstream_funcs}")
                self.repo.subjection_collector.send_data_to_waiting_downstream(state.transaction_id, function_name, downstream_funcs)
                crosstx_jobs = [
                            gevent.spawn(self.trigger_function_cross_tx, func_info)
                            for func_info in downstream_funcs
                ]   
            else:
                self.repo.subjection_collector.set_state_and_get_waiting_downstream(state.transaction_id, function_name, RUNNING)
                 
        # clear parent cnt and run state. For repairing. Remove the repair state of this function.
        state.lock.acquire()
        if not state.repair:
            state.parent_executed[function_name] = 0
            state.executed[function_name] = False
        
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
        #logging.info(f"running function {name}, REPAIR: {state.repair} transaction_id: {state.transaction_id}")
        res = self.function_manager.run(name, state.transaction_id, state.write_set, state.repair, state.repair_mode, state.batch_id, state.repair_states.get(name, {}))
        end = time.time()
        if res.get("Abort", False):
            logging.error(f"function {name} trigger abort: {res['error']}")
            return False
            
        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.

        if not state.repair:
            state.write_set.update(res["write_set"])
            self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start})
            self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']}) 
            state.container_port[name] = res['port']
            state.read_set[info["function_name"]] = res["read_set"]
            state.RYW_subjection[name] = res["RYW_subjection"]


        state.lock.release()
        return True

    def clear_mem(self, transaction_id):
        self.repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)