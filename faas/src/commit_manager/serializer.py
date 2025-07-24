from gevent import monkey
import gevent

monkey.patch_all()
from multiprocessing import Process
import time
import re
import validator_repo
from datetime import datetime
import logging

repo = validator_repo.Repository()
VALIDATE = 1
COMMIT = 3
CASCADED_COMMIT = 4

# 配置logging模块
def setup_logger():
    logger = logging.getLogger('serializer')
    logger.setLevel(logging.INFO)
    # 创建文件处理器
    handler = logging.FileHandler('/home/shao/FaaSnap/faas/logging/serializer.log', mode='a')
    handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    # 添加处理器到logger
    if not logger.handlers:
        logger.addHandler(handler)
    
    return logger

# 全局logger实例
logger = setup_logger()

def get_timestamp():
    # use timestamp as the version of batch.
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    return timestamp

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")

def log_message(message):
    logger.info(message)
    # 强制刷新缓冲区
    for handler in logger.handlers:
        handler.flush()

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
        self.commit_keys_per_batch = {}
        self.batch_validator_assignment = {}  # {batch_id: validator_worker_number}
        self.commit_suspended_batches = {} # {batch_id: validator_worker_number}
        self.function_pos = function_pos  # {func_name: {'ip': ip, 'port': port}}, used to get the ip of the function for commit.
        for func, ip in function_pos.items():
            self.function_pos[func] = extract_ip(ip)  # Extract IP without port
        
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
                commit_list_for_current_handler = []
                commit_keys_on_worker = {}
                batch_need_repair, expired_set, subjection_set, pessi_sink_info = self.accessed_set_validate(batch_id, version, data['transaction_list'], data['read_set'], data['write_set'])
                log_message(f"[VALIDATE] {batch_id}: need_repair={batch_need_repair}, expired_set={expired_set}, subjection_set={subjection_set}, pessi_sink_info={pessi_sink_info}")
                if not batch_need_repair:
                    self.commit_keys_per_batch[batch_id] = self.batch_write_info[batch_id]['writes']  # commit keys for this batch.
                    commit_list_for_current_handler, commit_keys_on_worker = self.commit_all_ready_batches(handler_id, batch_id)
                self.result_pipes[handler_id].put((batch_id, (batch_need_repair, expired_set, subjection_set, commit_list_for_current_handler, commit_keys_on_worker, pessi_sink_info)))
                
            elif op == COMMIT:
                self.commit_keys_per_batch[batch_id] = data['commit_keys']
                commit_list_for_current_handler, commit_keys_on_worker = self.commit_all_ready_batches(handler_id, batch_id)
                self.result_pipes[handler_id].put((batch_id, (commit_list_for_current_handler, commit_keys_on_worker)))

    # check if this batch is ready to commit.
    # if not, suspend this batch, and wait for its ancestors to finish.
    # in pessimistic mode, the batch is ready for sure: only flush the ready writes.
    def commit_all_ready_batches(self, current_handler_id, current_batch_id):
        ready, commit_list_per_handler, commit_keys_on_worker = self.get_commitable_batches(current_batch_id)
        log_message(f"[COMMIT] {current_batch_id} by handler {current_handler_id}: ready={ready}, commit_list_per_handler={commit_list_per_handler}, commit_keys_on_worker={commit_keys_on_worker}")
        commit_list_for_current_handler = commit_list_per_handler.pop(current_handler_id, [])
        if ready:
            for handler_id, commit_batch_list in commit_list_per_handler.items():
                self.handler_task_queues[handler_id].put(('', CASCADED_COMMIT, commit_batch_list))
        else:
            self.commit_suspended_batches[current_batch_id] = current_handler_id 
        return commit_list_for_current_handler, commit_keys_on_worker


    def accessed_set_validate(self, batch_id,version, transaction_list, read_set_per_batch, write_set_per_batch):
        expired_set = {}
        subjection_set = {}
        pessi_sink_info = {'batch_sub':{}, 'tx_sub':{}, 'last_tx':{}, 'whole_tx_sub':{}} # {'batch_sub':{'batch_id':[successors]}, 'tx_sub':{'tx_id':[successors]}}
        self.batch_write_info[batch_id] = {'version':version, 'ready_write_cnt':0, 'all_write_cnt':0, 'writes':{}}
        batch_need_repair = False
        tx_index_inside_batch = {tx_id: i for i, tx_id in enumerate(transaction_list)}
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
        pessi_nearest_writer = {'batch':(None, None), 'tx':None, 'tx_cross':None} # nearest writer info for pessimistic repair.
        whole_tx_sub = pessi_sink_info['whole_tx_sub']

        for func, kv_pairs in read_set.items():
            subjection_set[tx_id].setdefault(func, {"dirty":False, "up_cnt": 0, "upstream_keys": {}})
            expired_set[tx_id].setdefault(func, {})
            for key, version in kv_pairs.items():
                # key not written by any transaction before, check if it is expired.
                prev_writers = self.key_writers.get(key, [])
                # prev_writer_tuple: (batch_id, tx_id, func) 
                if not prev_writers:
                    # expired key.
                    if version < self.key_version_table.get(key):
                        expired_set[tx_id][func][key] = True
                        subjection_set[tx_id][func]["dirty"] = True
                        need_repair = True
                else:
                    need_repair = True
                    subjection_set[tx_id][func]["dirty"] = True
                    subjection_set[tx_id][func]["up_cnt"] += 1
                    prev_batch_id,  prev_tx_id,  prev_func = prev_writers[-1]
                    whole_tx_sub.setdefault(prev_tx_id, {})[tx_id] = True
                    subjection_set[tx_id][func]["upstream_keys"][key] = [prev_tx_id, prev_func]
                    # add to prev info.
                    if batch_id != prev_batch_id:
                        expired_set[tx_id][func][key] = True
                        if pessi_nearest_writer['batch'][0] is None or pessi_nearest_writer['batch'][1] < self.batch_write_info[prev_batch_id]['version']:
                            pessi_nearest_writer['batch'] = (prev_batch_id, self.batch_write_info[prev_batch_id]['version'])
                    else:
                        if pessi_nearest_writer['tx'] is None or tx_index_inside_batch[pessi_nearest_writer['tx']] < tx_index_inside_batch[prev_tx_id]:
                            pessi_nearest_writer['tx'] = prev_tx_id    
      
        nearest_batch = pessi_nearest_writer['batch'][0]
        nearest_tx = pessi_nearest_writer['tx']
        if nearest_batch:
            pessi_sink_info['batch_sub'].setdefault(nearest_batch, []).append(tx_id)
        if nearest_tx:
            pessi_sink_info['tx_sub'].setdefault(nearest_tx, []).append(tx_id)
            pessi_sink_info['last_tx'][tx_id] = nearest_tx
        return need_repair

    def update_key_writers(self, batch_id, tx_id, write_set):
        for key, writer_func in write_set.items():
            self.key_writers.setdefault(key, [])
            # update transaction writer count. 
            if key not in self.batch_write_info[batch_id]['writes']:
                self.batch_write_info[batch_id]['writes'][key] = True
                self.batch_write_info[batch_id]['all_write_cnt'] += 1
                if len(self.key_writers[key]) == 0:
                    self.batch_write_info[batch_id]['ready_write_cnt'] += 1
            if not self.key_writers[key] or self.key_writers[key][-1][0] != batch_id:
                # if the key is not written by this batch, add a new writer.
                self.key_writers[key].append((batch_id, tx_id, writer_func))
            else:
                # if the key is written by this batch, update the writer.
                self.key_writers[key][-1] = (batch_id, tx_id, writer_func)

    def prev_batch_committed(self, batch_id):
        # check if this batch is ready to commit.
        return self.batch_write_info[batch_id]['ready_write_cnt'] == self.batch_write_info[batch_id]['all_write_cnt']

    def get_commitable_batches(self, target_batch_id):
        if not self.prev_batch_committed(target_batch_id):
            log_message(f"[COMMIT] Batch {target_batch_id} is not ready to commit, waiting for ancestors to finish.")
            return False, {}, {}
        batches_ready_for_committing = [target_batch_id]
        commit_keys_on_worker = {} # {key: [(tx_id, func, version)]}
        commit_list_per_handler = {}  # {handler_id: [batch_id]}

        while batches_ready_for_committing:
            # add the first ready batch to commit list.
            current_batch_id = batches_ready_for_committing.pop(0)   
            current_batch_write_info = self.batch_write_info.pop(current_batch_id)
            current_batch_commit_keys = self.commit_keys_per_batch.pop(current_batch_id)
            current_handler_id = self.batch_validator_assignment.pop(current_batch_id)  
            version =  current_batch_write_info['version']
            # check cascaded batches: the writes are all ready.
            for key in current_batch_write_info['writes'].keys():
                log_message(f"[COMMIT] {current_batch_id} commit {key}:writers {self.key_writers[key]}")
                current_key_writers = self.key_writers[key]
                _,  writer_tx_id,  writer_func = current_key_writers.pop(0)
                if current_batch_commit_keys.pop(key, False):
                    self.key_version_table[key] = version
                    commit_keys_on_worker[key] = (writer_tx_id, writer_func, version)
                if len(current_key_writers) > 0:
                    cascaded_batch_id, _, _ = current_key_writers[0]
                    self.batch_write_info[cascaded_batch_id]['ready_write_cnt'] += 1
                    # only suspended batches are ready to commit cascaded.
                    if cascaded_batch_id in self.commit_suspended_batches and self.prev_batch_committed(cascaded_batch_id):    
                        self.commit_suspended_batches.pop(cascaded_batch_id)         
                        batches_ready_for_committing.append(cascaded_batch_id)
            commit_list_per_handler.setdefault(current_handler_id, []).append(current_batch_id)
        return True, commit_list_per_handler, commit_keys_on_worker


        


