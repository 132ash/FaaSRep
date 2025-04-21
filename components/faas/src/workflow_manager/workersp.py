import logging
import sys
from gevent import monkey
monkey.patch_all()
import time
import gevent.lock
import workersp_repo
from typing import Any, Dict, List
import requests
from validate_struct import ValidationQueue
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
    def __init__(self, transaction_id: str, all_func: List[str], function_pos, read_set, write_set, worker_set, batch_id, RYW_subjection, repair, repair_states, lock_set):
        self.transaction_id = transaction_id
        # {func: {key: version}}
        self.read_set:Dict[str:Dict[str:str]] = read_set
        # {key: func_ip}
        self.write_set:Dict[str:Dict[str:str]] = write_set
        # {func:{"down_funcs":[], "up_cnt":xx}
        self.RYW_subjection:Dict = RYW_subjection
        self.lock = gevent.lock.BoundedSemaphore() # guard the whole state
        self.executed: Dict[str, bool] = {}
        self.function_pos: Dict[str, str] = function_pos
        self.worker_set:Dict = worker_set
        self.parent_executed: Dict[str, int] = {}

        # repair state, used only when fast-path is turned off.
        self.repair = repair
        self.repair_states = repair_states # { func: {"RYW":{}, "dirty":False, "downstream": {"up_cnt": 0, "upstream_keys": {}}, "upstream":[]}}
        self.batch_id = batch_id
        self.function_pos_whole_batch = None

        # used only in remote lock mode.
        self.lock_set:Dict = lock_set

        for f in all_func:
            self.executed[f] = False
            self.parent_executed[f] = 0

    # a function is triggered by a cross-tx upstream function.
    # The function is dirty, and add parent cnt for start fucntion.
    def crosstx_trigger_modify(self, function_name):
        self.lock.acquire()
        func_repair_info = self.repair_states.get(function_name, {})
        func_repair_info['dirty'] = True
        self.parent_executed[function_name] += 1
        self.lock.release()

        

min_port = 20000

# mode: 'optimized' vs 'normal'
class WorkerSPManager:
    def __init__(self, host_addr: str, workflow_name: str, function_info_addr: str, validation_queue: ValidationQueue, reserve_pool:Dict) -> None:
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
        
        self.function_manager = FunctionManager(function_info_addr, min_port, self.node_list, reserve_pool)
        # repairing batches and finished transactions
        self.repair_table: Dict[str, int] = {}
        min_port += 5000

    # return the workflow state of the request
    def get_state(self, transaction_id: str, function_pos, read_set, write_set, worker_set, batch_id, RYW_subjection,  repair, repair_states, lock_set) -> TransactionState:
        self.lock.acquire()
        # first time to run, create new state
        if transaction_id not in self.states:
            self.states[transaction_id] = TransactionState(transaction_id, self.func, function_pos, read_set, write_set, worker_set, batch_id, RYW_subjection, repair, repair_states, lock_set)
        else:
            state = self.states[transaction_id]
            state.lock.acquire()
            if repair:
                state.function_pos_whole_batch = repo.get_global_function_pos(batch_id) if state.function_pos_whole_batch is None else state.function_pos_whole_batch
                state.repair = repair
                state.repair_states = repair_states 
                state.batch_id = batch_id
            else:
                state.lock_set.update(lock_set)
                state.function_pos.update(function_pos)
                state.worker_set.update(worker_set) 
                state.read_set.update(read_set)
                state.write_set.update(write_set)
                for func, RYW_info in RYW_subjection.items():
                    if func not in state.RYW_subjection:
                        state.RYW_subjection[func] = RYW_info
                    else:
                        state.RYW_subjection[func]["down_funcs"].update(RYW_info["down_funcs"])
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
    
    def validate_tx(self, workflow_name, transaction_id, read_set, write_set, function_pos, worker_set, RYW_subjection, lock_set):
        logging.info(f"Validating workflow:{workflow_name}, transaction_id: {transaction_id}, read_set:{read_set}, write_set:{write_set}, worker_set:{worker_set}, RYW_subjection:{RYW_subjection}, lock_set:{lock_set}")
        self.validation_queue.append(transaction_id, workflow_name, read_set, write_set, function_pos, worker_set, RYW_subjection, lock_set)

    def abort_tx(self, transaction_id, lock_set):
        # abort the transaction, waiting for re-run
        url = 'http://{}/notify'.format(config.GATEWAY_ADDR)
        logging.info(f'abort transaction:{transaction_id}, lock_set: {lock_set}')
        data = {"abort":True, 'transaction_id_list': [transaction_id], 'lock_set': lock_set}
        requests.post(url, json=data)
        return

    def trigger_repair(self, batch_id, transaction_id, function_name, no_parent_execution, port):
        base_url = 'http://127.0.0.1:{}/{}'
        data = {'batch_id':batch_id, 'transaction_id': transaction_id, "no_parent_execution": no_parent_execution, 'repair': True}
        try:
            requests.post(base_url.format(port, 'run'), json=data)
        except:
            state = self.states[transaction_id]
            info = self.get_function_info(function_name)
            self.function_manager.run({}, function_name, transaction_id, info['input'], info['output'], state.write_set, True, info['next'])

    # trigger the function when one of its parent is finished
    # function may run or not, depending on if all its parents were finished
    # function could be local or remote
    # with dirty set: the corresponding downstream is triggered, update dirty set.
    def trigger_function(self, state: TransactionState, function_name: str, no_parent_execution = False) -> None:
        if function_name == 'END':
            if not state.repair:
                self.validate_tx(self.workflow_name, state.transaction_id, state.read_set, state.write_set, state.function_pos, state.worker_set, state.RYW_subjection, state.lock_set)
            else:
                self.validation_queue.send_fin_repair_request(state.batch_id)
            return
        func_info = self.get_function_info(function_name)
        if func_info['ip'] == self.host_addr:
            self.trigger_function_local(state, function_name, func_info['ip'], no_parent_execution)
        else:
            # function runs on remote machine
            self.trigger_function_remote(state, function_name, func_info['ip'], no_parent_execution)

    # trigger a function that runs on local
    def trigger_function_local(self, state: TransactionState, function_name: str, ip:str, no_parent_execution = False) -> None:
        state.lock.acquire()
        if not no_parent_execution:
            state.parent_executed[function_name] += 1
        runnable = self.check_runnable(state, function_name)
        # remember to release state.lock
        if runnable:
            state.executed[function_name] = True
            if config.REPAIR and not state.repair:
                ip = extract_ip(ip)
                state.function_pos[function_name] = {'ip':ip, 'port':0}
                state.worker_set[ip] = True
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
            'function_pos':state.function_pos, 
            'worker_set': state.worker_set,
            'read_set': state.read_set,
            'write_set': state.write_set,
            'RYW_subjection':state.RYW_subjection,
            # get from validator. repair metadata.
            'batch_id': state.batch_id,
            'repair': state.repair,
            'repair_states': state.repair_states,
            # used in remote lock
            'lock_set': state.lock_set
        }
        requests.post(remote_url, json=data)

    def trigger_function_cross_tx(self, transaction_id, function_name, ip, batch_id):
        if not ip.endswith(":7000"):
            url = 'http://{}:7000/request'.format(ip)
        else:
            url = 'http://{}/request'.format(ip)
        data = {
            'transaction_id': transaction_id,
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
            up_cnt = state.repair_states.get(function_name, {}).get("downstream", {}).get('up_cnt', 0)
            upstream_cnt += up_cnt
            up_cnt = state.repair_states.get(function_name, {}).get("RYW", {}).get('up_cnt', 0)
            upstream_cnt += up_cnt
            logging.info(f"checking parents executed in repair. {function_name}, executed:{state.parent_executed[function_name]}, upstream_cnt: {upstream_cnt}, origin parent_cnt: {info['parent_cnt']}")
        # parent count: the sum of parents in workflow graph and parents in subject table
        return state.parent_executed[function_name] == info['parent_cnt'] + upstream_cnt and not state.executed[function_name] 

    # run a function on local
    def run_function(self, state: TransactionState, function_name: str) -> None:
        logging.info('run function: %s of: %s', function_name, state.transaction_id)
        # if function in repair mode and not dirty, skip running
        repair_metadata = state.repair_states.get(function_name, {})

        dirty = repair_metadata.get("dirty", False)
        info = self.get_function_info(function_name)
        # if function in repair mode and not dirty, skip running
        if not state.repair or dirty:
            successful, lock_set = self.run_normal(state, info)
            if not successful:
                self.del_state(state.transaction_id)
                self.abort_tx(state.transaction_id, lock_set)
                return

        # clear parent cnt and run state. For repairing. Remove the repair state of this function.
        state.parent_executed[function_name] = 0
        state.executed[function_name] = False
        state.repair_states.pop(function_name, {})
        
        # trigger downstream functions, including the ones in write relation table.
        jobs = [
            gevent.spawn(self.trigger_function, state, func)
            for func in info['next']
        ]
        if state.repair:
            RYW_sub = repair_metadata.get("RYW", {})
            downstream_sub = repair_metadata.get("upstream", [])
            logging.info(f"{function_name} REPAIR: trigger RYW func {RYW_sub}, crosstx func {downstream_sub}.")
            for func in RYW_sub.get('down_funcs', {}):
                print(f"function {function_name} RYW trigger downstream: {func}")
                jobs.append(gevent.spawn(self.trigger_function, state, func))
            for downstream_func_info in downstream_sub:
                downstream_txid = downstream_func_info['transaction_id']
                downstream_name = downstream_func_info['function_name']
                downstream_ip = state.function_pos_whole_batch[downstream_txid][downstream_name]['ip']
                print(f"function {function_name} crossrx trigger downstream: {downstream_name}")
                jobs.append(gevent.spawn(self.trigger_function_cross_tx, downstream_txid, downstream_name, downstream_ip, state.batch_id))
        gevent.joinall(jobs)

    def run_normal(self, state: TransactionState, info: Any) -> None:
        start = time.time()
        name = info['function_name']
        next_funcs = {} if state.repair else info['next']
        downstream_table = state.repair_states.get(name, {}).get("downstream", {})
        logging.info(f"running function {name}, REPAIR: {state.repair} transaction_id: {state.transaction_id}, write_set: {state.write_set}, downstream_table:{downstream_table}")
        res = self.function_manager.run(state.function_pos, name, state.transaction_id,
                             info['input'], info['output'], state.write_set, state.RYW_subjection.get(name, {}).get("upstream", {}), state.repair, 
                             next_funcs, info['parent_cnt'], state.batch_id, downstream_table.get('upstream_keys', {}), state.lock_set)
        end = time.time()
        if res.get("Error", False):
            logging.error(f"function {name} run error: {res['error']}")
            return False, res['lock_set']
            
        state.lock.acquire()
        # in first run, modify read/write set, func port, and update RYW relation.
        # only count the function latency in first run.
        
        if config.REPAIR and not state.repair:
            repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start})
            repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']})
            state.function_pos[name]['port'] = res['port']
            state.read_set[info["function_name"]] = res["read_set"]
            state.write_set.update(res["write_set"])
            # set RYW subjection table for itself if not exist.
            downstream_RYW_func_table = state.RYW_subjection.setdefault(name, {"down_funcs":{}, "up_cnt":0, "upstream":{}})
            # update RYW subjection table for upstream functions.
            for key, upstream_RYW_func in res["RYW_upstreams"].items():
                upstream_func_table = state.RYW_subjection.setdefault(upstream_RYW_func, {"down_funcs":{}, "up_cnt":0, "upstream":{}})
                upstream_func_table["down_funcs"][name] = True
                downstream_RYW_func_table["up_cnt"] += 1
                downstream_RYW_func_table["upstream"][key] = upstream_RYW_func
                logging.info(f"FIRST RUN, RYW info get from func: {res['RYW_upstreams']}, update RYW: {state.RYW_subjection}")
        if config.REMOTE_LOCK:
            # update lock set for the function
            repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'exec', 'time': end - start})
            repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'io', 'time': res['io_latency']})
            state.write_set.update(res["write_set"])
            state.lock_set.update(res['lock_set'])
            repo.save_latency({'transaction_id': state.transaction_id, 'function_name': info['function_name'], 'phase': 'lock', 'time': res['lock_latency']})
        state.lock.release()
        logging.info(f"function {info['function_name']} done, read_set: {res['read_set']}, write_set: {res['write_set']}, exec_latency: {end - start}, io_latency: {res['io_latency']}")

        return True, {}

    def clear_mem(self, transaction_id):
        repo.clear_mem(transaction_id)
    
    def clear_db(self, transaction_id):
        repo.clear_db(transaction_id)