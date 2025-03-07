from gevent import monkey
monkey.patch_all()
import sys
import logging
import time
import gevent.lock
import workersp_repo
from typing import Any, Dict, List
import requests
from validation_queue import ValidationQueue
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
    def __init__(self, transaction_id: str, all_func: List[str], function_pos: Dict[str, str]={}, read_set={}, write_set={}, repair=False, repair_states={}):
        self.transaction_id = transaction_id
        # {func: {key: version}}
        self.read_set:Dict[str:Dict[str:str]] = read_set
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func: []]}
        self.expired_keys = repair_states['expired_keys'] # {ip：list（keys)}
        self.downstream_func_table = repair_states['downstream_func_table'] #{func:{up_cnt:xxx,upstream_keys:{key:{upstream_func:xx, upstream_ip:xx}}}}
        self.upstream_func_table = repair_states['upstream_func_table'] # {func: [{func_name:xxx, ip:xx, transaction_id, xxx, workflow_name:xx}]}
        self.RYW_subjection = repair_states['RYW_subjection'] #{func:{"down_funcs":[], "up_cnt":xx}
        self.dirty_set = repair_states['dirty_set'] # [funcs]
        
        self.repair = repair # if the transaction is in repair mode
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.function_pos: Dict[str, str] = function_pos
        self.parent_executed: Dict[str, int] = {}
        self.batch_id = 0

        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0

    # a function is triggered by a cross-tx upstream function.
    # The function is dirty, and add parent cnt for start fucntion.
    def crosstx_trigger_modify(self, function_name, no_parent_execution):
        self.lock.acquire()
        self.dirty_set[function_name] = True
        if no_parent_execution:
            self.parent_executed[function_name] += 1
        self.lock.release()
        

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str, validation_queue: ValidationQueue) -> None:
        global min_port

        self.lock = gevent.lock.BoundedSemaphore() # guard self.states
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        self.states: Dict[str, TransactionState] = {}
        self.function_info: Dict[str, dict] = {}

        self.info_db = workflow_name + '_function_info'
        self.common_db = 'common'
        self.meta_db = workflow_name + '_workflow_metadata'
        self.validation_queue = validation_queue

        self.func = repo.get_current_node_functions(self.host_addr, self.info_db)
        self.node_list = repo.get_all_addrs(self.common_db)
        
        self.function_manager = FunctionManager(function_info_addr, min_port, self.node_list)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000

    # return the workflow state of the request
    def get_state(self, transaction_id: str, function_pos, read_set, write_set, repair, repair_states) -> TransactionState:
        self.lock.acquire()
        # first time to run, create new state
        if transaction_id not in self.states:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, function_pos, read_set, write_set, repair, repair_states)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            state.function_pos.update(function_pos)
            state.RYW_subjection.update(repair_states['RYW_subjection'])
            # update repair message from upstream.
            if repair:
                state.repair = repair
                state.expired_keys = repair_states['expired_keys'] 
                state.batch_id = repair_states['batch_id']
                state.dirty_set.update(repair_states['dirty_set'])
                state.downstream_func_table.update(repair_states['downstream_func_table'])
                state.upstream_func_table.update(repair_states['upstream_func_table'])
                # in repair, don't modify read_set and write_set
            else:
                state.read_set.update(read_set)
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

    # get function's info from database
    # the result is cached
    def get_function_info(self, function_name: str) -> Any:
        if function_name not in self.function_info:
            self.function_info[function_name] = repo.get_function_info(function_name, self.info_db)
        return self.function_info[function_name]
    
    def validate_tx(self, workflow_name, transaction_id, read_set, write_set, function_pos, RYW_subjection):
        print(f"Validating workflow:{workflow_name}, transaction_id: {transaction_id}, read_set:{read_set}, write_set:{write_set}, function_pos:{function_pos}, RYW_subjection:{RYW_subjection}")
        self.validation_queue.append(transaction_id, workflow_name, read_set, write_set, function_pos, RYW_subjection)


    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if state.repair:
                self.validation_queue.send_fin_repair_request(state.batch_id)
            else:
                self.validate_tx(self.workflow_name, state.transaction_id, state.read_set, state.write_set, state.function_pos, state.RYW_subjection)
            return
        func_info = self.get_function_info(function_name)
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, func_info['ip'], no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str, ip:str, no_parent_execution = False) -> None:
        print(f'trigger local function: {function_name} of: {state.transaction_id}')
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
        print(f'trigger remote function: {function_name} on: {remote_addr} of: {state.transaction_id}')
        remote_url = 'http://{}/request'.format(remote_addr)
        data = {
            'transaction_id': state.transaction_id,
            'workflow_name': self.workflow_name,
            'function_name': function_name,
            'no_parent_execution': no_parent_execution,
            'function_pos':state.function_pos, 
            'read_set': state.read_set,
            'write_set': state.write_set,
            'repair': state.repair,
            'expired_keys': state.expired_keys,
            'dirty_set': state.dirty_set,
            'downstream_func_table': state.downstream_func_table,
            'upstream_func_table': state.upstream_func_table,
            'batch_id': state.batch_id,
            'RYW_subjection':state.RYW_subjection
        }
        response = requests.post(remote_url, json=data)
        response.close()

    def trigger_function_cross_tx(self, transaction_id, workflow_name, function_name, ip, batch_id):
        if not ip.endswith(":7000"):
            url = 'http://{}:7000/request'.format(ip)
        else:
            url = 'http://{}/request'.format(ip)
        data = {
            'transaction_id': transaction_id,
            'workflow_name': workflow_name,
            'function_name': function_name,
            'no_parent_execution': False,
            'repair': True,
            'batch_id': batch_id,
            'crosstx': True
        }
        requests.post(url, json=data)

    
    # check if a function's parents are all finished
    # If in repair mode, add upstream parents 
    def check_runnable(self, state: TransactionState, function_name: str) -> bool:
        info = self.get_function_info(function_name)
        upstream_cnt = 0
        if state.repair:
            if function_name in state.downstream_func_table:
                upstream_cnt += state.downstream_func_table[function_name]["up_cnt"]
            if function_name in state.RYW_subjection:
                upstream_cnt += state.RYW_subjection[function_name]['up_cnt']
            print(f"checking repairing {function_name}, executed:{state.parent_executed[function_name]}, upstream_cnt: {upstream_cnt}, origin parent_cnt: {info['parent_cnt']}")
        # parent count: the sum of parents in workflow graph and parents in subject table
        return state.parent_executed[function_name] == info['parent_cnt'] + upstream_cnt and not state.executed[function_name] 

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        logging.info('run function: %s of: %s', function_name, state.transaction_id)
        dirty = state.dirty_set.get(function_name, False)
        # if function in repair mode and not dirty, skip running
        if not state.repair or dirty:
            info = self.get_function_info(function_name)
            self.run_normal(state, info)
        
        # clear parent cnt and run state.. For repairing.
        state.parent_executed[function_name] = 0
        state.executed[function_name] = False
        
        # trigger downstream functions, including the ones in write relation table.
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]
        # trigger the functions subject to a write. inter or intro tx.
        # inter tx: no need to update dirty set(must be expired). just trigger.
        if state.repair:
            RYW_sub = state.RYW_subjection.pop(function_name, {})
            downstream_sub = state.upstream_func_table.pop(function_name, [])
            state.upstream_func_table.pop(function_name, {})
            state.dirty_set.pop(function_name, {})
            print(f"REPAIR: RYW subjection table for func{function_name}: {RYW_sub}")
            for func in RYW_sub['down_funcs']:
                print(f"function{function_name} RYW trigger downstream: {func}")
                jobs.append(gevent.spawn(self.trigger_function, state, func))
            for downstream_func_info in downstream_sub:
                ip = downstream_func_info['ip']
                txid = downstream_func_info['transaction_id']
                downstream_name = downstream_func_info['function_name']
                workflow_name = downstream_func_info['workflow_name']
                print(f"function {function_name} crossrx trigger downstream: {downstream_name}")
                jobs.append(gevent.spawn(self.trigger_function_cross_tx, txid, workflow_name, downstream_name, ip, state.batch_id))
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        name = info['function_name']
        if state.repair:
            print(f"RUN FUNC: repairing {name}, downstream_func_table:{state.downstream_func_table.get(name, {})}")
        res = self.function_manager.run(state.function_pos, name, state.transaction_id,
                             info['input'], info['output'], state.write_set, state.repair, 
                             state.downstream_func_table.get(name, {}).get('upstream_keys', {}))
        end = time.time()
        state.lock.acquire()
        # in first run, modify read/write set, and update RYW relation.
        if not state.repair:
            state.read_set[info["function_name"]] = res["read_set"]
            state.write_set.update(res["write_set"])
            # set RYW subjection table for itself if not exist.
            if name not in state.RYW_subjection:
                state.RYW_subjection[name] = {"down_funcs":[], "up_cnt":0}
            # update RYW subjection table for upstream functions.
            for upstream_RYW_func in res["RYW_upstreams"].keys():
                if upstream_RYW_func not in state.RYW_subjection:
                    state.RYW_subjection[upstream_RYW_func] = {"down_funcs":[], "up_cnt":0}
                state.RYW_subjection[upstream_RYW_func]["down_funcs"].append(name)
                state.RYW_subjection[name]["up_cnt"] += 1
                print(f"update RYW subjection table, now: {state.RYW_subjection}")
        # in repair mode, state is consumed, and downstream functions are affected.
        else:
            for downstream_func in state.RYW_subjection[name]["down_funcs"]:
                state.dirty_set[downstream_func] = True
        state.lock.release()
        print(f"function {info['function_name']} done, read_set: {res['read_set']}, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")
        repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start})
        repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']})

    def clear_mem(self, transaction_id):
        repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        repo.clear_db(transaction_id)