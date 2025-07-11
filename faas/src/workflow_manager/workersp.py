import logging
import sys
from gevent import monkey
monkey.patch_all()
import time
import gevent.lock
from workersp_repo import Repository
from typing import Any, Dict, List
import requests
from validate_struct import TransactionSink
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
    def __init__(self, transaction_id: str, all_func: List[str], container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_states, lock_set, snapshot_interval):
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
        self.repair_states = repair_states 
        self.batch_id = batch_id

        # used only in remote lock mode.
        self.lock_set:Dict = lock_set
        self.snapshot_interval:list = snapshot_interval


        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0
        

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str, transaction_sink: TransactionSink, reserve_pool:Dict, repo:Repository, node_list:list) -> None:
        global min_port

        self.info_db = workflow_name + '_function_info'
        self.common_db = 'common'
        self.meta_db = workflow_name + '_workflow_metadata'

        self.lock = gevent.lock.BoundedSemaphore() # guard self.states
        self.host_addr = host_addr
        repo.shadowtable_init(extract_ip(host_addr))
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.func = self.repo.get_current_node_functions(self.host_addr, self.info_db)
        self.function_info = {}
        self.function_pos = {}
        for function_name in self.func:
            self.function_info[function_name] = self.repo.get_function_info(function_name, self.info_db)
            self.function_pos[function_name] = self.function_info[function_name]['ip']

        self.transaction_sink = transaction_sink
        self.repo = repo
        self.transaction_sink_addr = self.function_pos[self.repo.get_end_function(self.meta_db)]

        self.node_list = node_list
        
        self.function_manager = FunctionManager(function_info_addr, self.transaction_sink_addr, min_port, self.node_list, reserve_pool, self.function_pos)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000
        # config of different modes.
        self.BASIC = config.BASIC
        self.FAST_PATH = config.FAST_PATH
        self.REMOTE_LOCK = config.REMOTE_LOCK
        self.REPAIR = config.REPAIR
        self.FAASTCC = config.FAASTCC
        self.CONCORD = config.CONCORD
        self.OPTIMISTIC_REPAIR = config.OPTIMISTIC_REPAIR

    # return the workflow state of the request
    def get_state(self, retry_after_abort, transaction_id: str, container_port, read_set, write_set, batch_id, RYW_subjection,  repair, repair_states, lock_set, snapshot_interval) -> TransactionState:
        self.lock.acquire()
        # first time to run or retry trigggered by gateway, create new state.
        if transaction_id not in self.states or retry_after_abort:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, container_port, read_set, write_set, batch_id, RYW_subjection, repair, repair_states, lock_set, snapshot_interval)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            if repair:
                state.repair = repair
                state.repair_states = repair_states 
                state.batch_id = batch_id
            else:
                if self.FAASTCC:
                    min_snapshot = max(snapshot_interval[0], state.snapshot_interval[0])
                    max_snapshot = min(snapshot_interval[1], state.snapshot_interval[1])
                    if min_snapshot > max_snapshot:
                        state.lock.release()
                        return None
                    state.snapshot_interval = [min_snapshot, max_snapshot]
                state.lock_set.update(lock_set)
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


    def FaaSTCC_Concord_commit(self, transaction_id: str, write_set: Dict[str, int], read_set) -> None:
        commiter_url = 'http://{}:7000/commit'.format(self.VALIDATOR_ADDR)
        data = {
            'transaction_id': transaction_id,
            'workflow_name': self.workflow_name,
            'write_set': write_set,
            'read_set': read_set,
        }
        requests.post(commiter_url, json=data)
    
    def validate_tx(self, workflow_name, transaction_id, read_set, write_set, container_port, RYW_subjection, lock_set, snapshot_interval) -> None:
        logging.info(f"Validating workflow:{workflow_name}, transaction_id: {transaction_id}, read_set:{read_set}, write_set:{write_set}, RYW_subjection:{RYW_subjection}, lock_set:{lock_set}, snapshot_interval:{snapshot_interval}")
        self.transaction_sink.append(transaction_id, workflow_name, read_set, write_set, container_port, RYW_subjection, lock_set, snapshot_interval)

    def abort_tx(self,transaction_id, batch_id='', lock_set={}):
        # trigger next run of the transaction under pessimistic repair mode
        url = 'http://{}:7000/abort'.format(self.transaction_sink_addr)
        data = {'batch_id':batch_id, 'transaction_id': transaction_id, 'workflow_name': self.workflow_name, 'lock_set':lock_set}
        requests.post(url, json=data)

    def trigger_repair(self, batch_id, transaction_id, function_name, no_parent_execution, port):
        base_url = 'http://127.0.0.1:{}/{}'
        data = {'batch_id':batch_id, 'transaction_id': transaction_id, "no_parent_execution": no_parent_execution, 'repair': True}
        try:
            requests.post(base_url.format(port, 'run'), json=data)
        except:
            state = self.states[transaction_id]
            info = self.function_info[function_name]
            self.function_manager.run({}, function_name, transaction_id, info['input'], info['output'], state.write_set, True, info['next'])

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if not state.repair:
                if self.FAASTCC or self.CONCORD:
                    self.FaaSTCC_Concord_commit(state.transaction_id, state.write_set, state.read_set)
                else:
                    self.validate_tx(self.workflow_name, state.transaction_id, state.read_set, state.write_set, state.container_port, state.RYW_subjection, state.lock_set, state.snapshot_interval)
            else:
                self.transaction_sink.fin_repair_or_abort(state.batch_id, state.transaction_id, REPAIRED)
            return
        func_info = self.function_info[function_name]
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    def crosstx_trigger_function(self, transaction_id: str, function_name: str) -> None:
        state = self.states[transaction_id]
        func_info = self.function_info[function_name]
        self.trigger_function_local(state, function_name, func_info['ip'])

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str,  no_parent_execution = False) -> None:
        state.lock.acquire()
        if state.stop_running:
            return
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        if state.repair:
            # fetch subjection from redis, used in optimistic repair mode
            upstream_fetch_info = self.repo.subjection_collector.fetch_upstream_keys(state.repair_states[function_name]["upstream_keys"], state.transaction_id, function_name) 
            upstream_waiting_count = self.repo.subjection_collector.prepair_subjection_before_repair(state.transaction_id, function_name, state.repair_states[function_name]["upstream_keys"],upstream_fetch_info ) 
            state.repair_states[function_name]["up_cnt"] = upstream_waiting_count     
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
            'repair_states': state.repair_states,
            # used in remote lock
            'lock_set': state.lock_set,
            'snapshot_interval': state.snapshot_interval
        }
        requests.post(remote_url, json=data)

    def trigger_function_cross_tx(self, func_info):
        downstream_tx_id, function_name, remote_addr = func_info[0], func_info[1], func_info[2]
        remote_url = 'http://{}:7000/crosstx_req'.format(remote_addr)
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
            up_cnt = state.repair_states.get(function_name, {}).get('up_cnt', 0)
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
            successful, lock_set = self.run_normal(state, info)
            if not successful:
                self.abort_tx(state.transaction_id,state.batch_id,lock_set)
                return
            
        if self.OPTIMISTIC_REPAIR and state.repair:
            downstream_funcs = self.repo.subjection_collector.set_state_and_get_waiting_downstream(state.transaction_id, function_name, REPAIRED)
            self.repo.subjection_collector.send_data_to_waiting_downstream(state.transaction_id, function_name, downstream_funcs)
            crosstx_jobs = [
                        gevent.spawn(self.trigger_function_cross_tx, func_info)
                        for func_info in downstream_funcs
            ]    
                 
        # clear parent cnt and run state. For repairing. Remove the repair state of this function.
        state.lock.acquire()
        if state.stop_running:
            return
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
        logging.info(f"running function {name}, REPAIR: {state.repair} transaction_id: {state.transaction_id}, write_set: {state.write_set}")
        res = self.function_manager.run(name, state.transaction_id, state.write_set, state.repair, state.batch_id, state.lock_set, state.repair_states.get(name, {}), state.snapshot_interval)
        end = time.time()
        if res.get("Abort", False):
            logging.error(f"function {name} trigger abort: {res['error']}")
            return False, res['lock_set']
            
        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.

        state.write_set.update(res["write_set"])
        if self.CONCORD:
            state.read_set.update(res["read_set"])
        if not state.repair:
            self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start})
            self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']}) 
            if self.REPAIR:
                state.container_port[name] = res['port']
                state.read_set[info["function_name"]] = res["read_set"]
                state.RYW_subjection[name] = res["RYW_subjection"]
                logging.info(f"FIRST RUN, RYW info get from func: {res['RYW_upstreams']}, update RYW: {state.RYW_subjection}")

        if self.REMOTE_LOCK:
            # update lock set for the function
            state.lock_set.update(res['lock_set'])
            self.repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'lock', 'time': res['lock_latency']})
        
        if self.FAASTCC:
            state.snapshot_interval = res['snapshot_interval']
        state.lock.release()
        logging.info(f"function {info['function_name']} done, read_set: {res['read_set']}, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")

        return True, res['lock_set']

    def clear_mem(self, transaction_id):
        self.repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        self.repo.clear_db(transaction_id)