from gevent import monkey
import gevent

monkey.patch_all()
from multiprocessing import Process, Queue, Pipe
import time
import sys
import validator_repo
from collections import defaultdict
from datetime import datetime

repo = validator_repo.Repository()
sys.path.append('../../config')
import config
PESSIMISTIC_REPAIR = config.PESSIMISTIC_REPAIR
VALIDATE = 1
COMMIT = 2
CASCADED_COMMIT = 3

def get_timestamp():
    # use timestamp as the version of batch.
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    return timestamp

class SerializerProcess(Process):
    def __init__(self, req_queue, result_pipes, handler_task_queues, function_pos):
        super().__init__()
        self.req_queue = req_queue
        self.result_pipes = result_pipes 
        self.handler_task_queues = handler_task_queues  # {handler_id: task_queue}, used to trigger seq commits  
        self.key_version_table = repo.get_initial_global_table() # {key: version}
        self.key_writers = {}   # {key: [('batch_id':xx, 'tx_id':xx, 'func':xx)]}                                                   # {key: [{'batch_id',xx, 'tx_id':xx, 'func':xx}] }
        # infomation for commiting batches.
        self.batch_write_info = {} # {batch_id: {version, ready_write_cnt, all_write_cnt, writes:{key: False}}}, used for commit
        self.batch_validator_assignment = {}  # {batch_id: validator_worker_number}
        self.commit_suspended_batches = {} # {batch_id: validator_worker_number}
        self.function_pos = function_pos  # {func_name: {'ip': ip, 'port': port}}, used to get the ip of the function for commit.
        
    def run(self):
        last_task_time = time.time()
        while True:
            try:
                msg = self.req_queue.get(timeout=1)
                last_task_time = time.time()
            except:
                # 1秒无任务则休眠
                if time.time() - last_task_time > 1:
                    gevent.sleep(0.1)
                continue
            handler_id, batch_id, op, data = msg
            # find dirty set, and subjection set send to the validator to repair.
            # if the batch is ready to commit, send the commit list to the handler.
            if op == VALIDATE:
                self.batch_validator_assignment[batch_id] = handler_id
                version = get_timestamp()
                batch_need_repair, expired_set, subjection_set, pessi_sink_info = self.accessed_set_validate(batch_id, version, data['transaction_list'], data['read_set'], data['write_set'])
                if not batch_need_repair:
                    commit_list_for_current_handler = self.commit_all_ready_batches(batch_id)
                self.result_pipes[handler_id].put((batch_need_repair, expired_set, subjection_set, commit_list_for_current_handler, pessi_sink_info))
            elif op == COMMIT:
                commit_list_for_current_handler = self.commit_all_ready_batches(handler_id, batch_id)
                self.result_pipes[handler_id].put(commit_list_for_current_handler)

    # check if this batch is ready to commit.
    # if not, suspend this batch, and wait for its ancestors to finish.
    # in pessimistic mode, the batch is ready for sure: only flush the ready writes.
    def commit_all_ready_batches(self, current_handler_id, current_batch_id):
        ready, commit_list_per_handler = self.get_commitable_batches(current_batch_id)
        commit_list_for_current_handler = commit_list_per_handler.pop(current_handler_id, [])
        if ready:
            for handler_id, commit_batch_list in commit_list_per_handler.items():
                self.handler_task_queues[handler_id].put(('', CASCADED_COMMIT, commit_batch_list))
        else:
            self.commit_suspended_batches[current_batch_id] = current_handler_id 
        return commit_list_for_current_handler


    def accessed_set_validate(self, batch_id,version, transaction_list, read_set_per_batch, write_set_per_batch):
        expired_set = {}
        subjection_set = {}
        pessi_sink_info = {'batch_sub':{}, 'tx_sub':{}} # {'batch_sub':{'batch_id':[successors]}, 'tx_sub':{'tx_id':[successors]}}
        self.batch_write_info[batch_id] = {'version':version, 'ready_write_cnt':0, 'all_write_cnt':0, 'writes':{}}
        batch_need_repair = False
        tx_index_inside_batch = {tx_id: i for i, tx_id in enumerate(transaction_list)} if config.PESSIMISTIC_REPAIR else None
        for tx_id in transaction_list:
            expired_set[tx_id] = {}
            subjection_set[tx_id] = {}
            rs = read_set_per_batch[tx_id]
            tx_need_repair = self.get_expired_set_and_subjection(batch_id, tx_id, expired_set, subjection_set, rs, pessi_sink_info, tx_index_inside_batch)
            if tx_need_repair:
                batch_need_repair = True
            self.update_key_writers(batch_id, tx_id, write_set_per_batch[tx_id])
        return batch_need_repair, expired_set, subjection_set, pessi_sink_info


    def get_expired_set_and_subjection(self,batch_id, tx_id, expired_set, subjection_set, read_set, pessi_sink_info, tx_index_inside_batch:dict):
        need_repair = False
        pessi_nearest_writer = {'batch':(None, None), 'tx':None} # nearest writer info for pessimistic repair.
            
        for func, kv_pairs in read_set.items():
            subjection_set[tx_id].setdefault(func, {"dirty":False, "up_cnt": 0, "upstream_keys": {}})
            expired_set[tx_id].setdefault(func, {})
            for key, version in kv_pairs.items():
                # key not written by any transaction before, check if it is expired.
                prev_writer_tuple = self.key_writers.get(key, None)
                # prev_writer_tuple: (batch_id, tx_id, func) 
                if not prev_writer_tuple:
                    # expired key.
                    if version < self.key_version_table.get(key):
                        expired_set[tx_id][func][key] = True
                        subjection_set[tx_id][func]["dirty"] = True
                        need_repair = True
                else:
                    need_repair = True
                    subjection_set[tx_id][func]["dirty"] = True
                    prev_batch_id,  prev_tx_id,  prev_func, prev_ip = prev_writer_tuple
                    if PESSIMISTIC_REPAIR:
                        # add to prev info.
                        if batch_id != prev_batch_id:
                            expired_set[tx_id][func][key] = True
                            if pessi_nearest_writer['batch'][0] is None or pessi_nearest_writer['batch'][1] < self.batch_write_info[prev_batch_id]['version']:
                                pessi_nearest_writer['batch'] = (prev_batch_id, self.batch_write_info[prev_batch_id]['version'])
                        else:
                            if pessi_nearest_writer['tx'] is None or pessi_nearest_writer['tx'] < tx_index_inside_batch(prev_tx_id):
                                pessi_nearest_writer['tx'] = prev_tx_id    
                    else: 
                        subjection_set[tx_id][func]["upstream_keys"][key] = {'tx_id': prev_tx_id, 'func': prev_func, 'ip':prev_ip}
            pessi_sink_info['batch_sub'].setdefault(pessi_nearest_writer['batch'][0], []).append(tx_id)
            pessi_sink_info['tx_sub'].setdefault(pessi_nearest_writer['tx'], []).append(tx_id)
            pessi_sink_info['last_tx'][tx_id] = pessi_nearest_writer['tx']
            return need_repair

    def update_key_writers(self, batch_id, tx_id, write_set):
        for key, writer_func in write_set.items():
            self.key_writers.setdefault(key, [])
            # update transaction writer count. 
            if not self.batch_write_info[batch_id]['writes'].get(key, False):
                self.batch_write_info[batch_id]['writes'][key] = True
                self.batch_write_info[batch_id]['all_write_cnt'] += 1
                if len(self.key_writers[key]) == 0:
                    self.batch_write_info[batch_id]['ready_write_cnt'] += 1
                self.key_writers[key].append((batch_id, tx_id, writer_func, self.function_pos[writer_func]))

    def prev_batch_committed(self, batch_id):
        # check if this batch is ready to commit.
        return self.batch_write_info[batch_id]['ready_write_cnt'] == self.batch_write_info[batch_id]['all_write_cnt']

    def get_commitable_batches(self, target_batch_id):
        if not self.prev_batch_committed(current_batch_id):
            return False, {}
        batches_ready_for_committing = [target_batch_id]
        commit_list_per_handler = {}

        while batches_ready_for_committing:
            # add the first ready batch to commit list.
            current_batch_id = batches_ready_for_committing.pop(0)   
            current_batch_write_info = self.batch_write_info.pop(current_batch_id)
            current_handler_id = self.batch_validator_assignment.pop(current_batch_id) 
            keys_for_commit_per_ip = defaultdict(list)   
            # check cascaded batches: the writes are all ready.
            for key in current_batch_write_info['writes'].keys():
                current_key_writers = self.key_writers[key]
                _,  writer_tx_id,  writer_func, writer_ip = current_key_writers.pop(0)
                self.key_version_table[key] = current_batch_write_info['version']
                if not PESSIMISTIC_REPAIR:
                    keys_for_commit_per_ip[writer_ip].append(f"{writer_tx_id}:PUT:{writer_func}:{key}")
                    if len(current_key_writers) > 0:
                        cascaded_batch_id, _, _ = current_key_writers[0]
                        self.batch_write_info[cascaded_batch_id]['ready_write_cnt'] += 1
                        # only suspended batches are ready to commit cascaded.
                        if self.prev_batch_committed(cascaded_batch_id) and cascaded_batch_id in self.commit_suspended_batches:    
                            self.commit_suspended_batches.pop(cascaded_batch_id)         
                            batches_ready_for_committing.append(cascaded_batch_id)
            commit_list_per_handler.setdefault(current_handler_id, []).append((current_batch_id, current_batch_write_info['version'], keys_for_commit_per_ip))
        return True, commit_list_per_handler


        


