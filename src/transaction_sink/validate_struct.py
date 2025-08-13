from gevent import monkey
monkey.patch_all()
import gevent
import os
import requests
import logging
from typing import Dict
import sys
import time
from batch_state_struct import PessimisticBatchState, OptimisticTransactionState
import gevent.lock
import gevent.queue  # 添加 gevent 队列导入
sys.path.append('../../config')
import config

REPAIRED = config.REPAIRED
ABORTED = config.ABORTED    
WAITING = config.RUNNING

PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR
VALIDATOR_ADDR = config.VALIDATOR_ADDR
VALIDATE_INTERVAL = config.VALIDATE_INTERVAL
BATCH_TIMEOUT = config.BATCH_TIMEOUT # 50ms

log_file = '../../logging/sink.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

def setup_logger():
    logger = logging.getLogger('sink')
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

class RepairingBatchState:
    def __init__(self, workflow_name):
        self.pessimistic_state_per_batch:Dict[str, PessimisticBatchState] = {}
        self.optimistic_state_per_transaction:Dict[str, OptimisticTransactionState] = {}
        self.subjection_between_transactions = {} # {prev_batch_id: {next_batch_id: [txs]}}
        self.transaction_list_per_batch = {}
        self.tx_finished_table_per_batch = {}
        self.state_lock  = gevent.lock.BoundedSemaphore()
        self.workflow_name = workflow_name

    def register_batch(self, batch_id, tx_list, batch_size):
        self.transaction_list_per_batch[batch_id] = tx_list
        self.tx_finished_table_per_batch[batch_id] = {'total':batch_size, "finished": 0}     
        self.pessimistic_state_per_batch[batch_id] = PessimisticBatchState(batch_id, tx_list, batch_size)
        for tx_id in tx_list:
            self.optimistic_state_per_transaction[tx_id] = OptimisticTransactionState(batch_id, tx_id)
        #log_message(f"[REPAIR REGISTER] tx_finished_table_per_batch: {self.tx_finished_table_per_batch}")

    def update_subjection_info(self, batch_id:str, batch_sub, tx_sub, sub_per_tx_optimistic={}):
        """
        Update the subjection info for the given batch. 
        batch_sub: {prev_batch_id:[txs]}
        tx_sub: {prev_batch_id: [txs]}
        """
        ready_txs = {txid: True for txid in self.transaction_list_per_batch[batch_id]}
        batch_successors = []
        for prev_batch_id, next_txs in batch_sub.items():
            prev_batch_info = self.pessimistic_state_per_batch.get(prev_batch_id, None)
            if prev_batch_info:
                #log_message(f"[UPDATE SUB TO PREV BATCH] batch_id {batch_id} SUB TO prev_batch_id {prev_batch_id}, prev_batch_info: {prev_batch_info}")
                self.pessimistic_state_per_batch[prev_batch_id].modify_batch_successors(batch_id, next_txs, batch_successors)
        self.pessimistic_state_per_batch[batch_id].init_tx_info(ready_txs, tx_sub, batch_successors)
        # update optimistic subjection
        opt_txs_become_pessi = {}
        for prev_transaction_id, next_txs in sub_per_tx_optimistic.items():
            prev_tx_opt_state = self.optimistic_state_per_transaction.get(prev_transaction_id, None)
            if prev_tx_opt_state:
                next_txs = list(next_txs.keys())
                prev_repair_state = self.optimistic_state_per_transaction[prev_transaction_id].modify_transaction_subjection(next_txs)
                if prev_repair_state == ABORTED:
                    for next_tx in next_txs:
                        opt_txs_become_pessi[next_tx] = True
                        self.optimistic_state_per_transaction[next_tx].need_pessimistic_repair = True
        #log_message(f"[BECOME PESSIMISTIC] In batch_id {batch_id}, {opt_txs_become_pessi} needs pessi repair.")     
        return ready_txs, opt_txs_become_pessi

    def clear_opt_table_after_finish(self, batch_id_list):
        for batch_id in batch_id_list:
            for tx_id in self.transaction_list_per_batch[batch_id]:
                self.optimistic_state_per_transaction.pop(tx_id, None)
            self.transaction_list_per_batch.pop(batch_id, None)

    def reminder_successor_tx_pessi(self, batch_id, tx_id, batch_finished):
        ready_txs = {}
        if batch_finished:
            batch_trigger_txs = self.pessimistic_state_per_batch[batch_id].next_txs_after_batch
            for next_batch_id, next_trigger_txs in batch_trigger_txs.items():
                self.pessimistic_state_per_batch[next_batch_id].trigger_successor(next_trigger_txs, ready_txs)
        else:
            self.pessimistic_state_per_batch[batch_id].transaction_finish(tx_id, ready_txs)
        #log_message(f"[PES REMINDER NEXT] batch_id {batch_id} finished:{batch_finished}, and tx_id: {tx_id} fin, {ready_txs} is pessi ready.")
        return ready_txs 
       
    def after_transaction_finish(self, origin_batch_id, repair_mode, tx_id, state, skip_repair):
        finished_txs_and_state = []
        tasks_tx_finish_repair = {} # {batch_id: {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]}}
        tasks_cascaded_repair = {}
        if skip_repair:
            finished_txs_and_state = [(origin_batch_id, self.transaction_list_per_batch[origin_batch_id][-1], state)]
            self.tx_finished_table_per_batch[origin_batch_id]["finished"] = self.tx_finished_table_per_batch[origin_batch_id]["total"] - 1
        else:
            if not PESSIMISTIC_REPAIR:
                rejected, successors_to_be_pessimistic = self.optimistic_state_per_transaction[tx_id].optimistic_state_change_after_repair(repair_mode, state)
                if rejected:
                    #log_message(f"[OPTIMISTIC REPAIR REJECTED] Opt repair of Transaction {tx_id} in batch {origin_batch_id} is rejected, state: {state}, it needs pessi repair.")
                    return False, []
                if state == ABORTED:
                    for next_tx_id in successors_to_be_pessimistic:
                        #log_message(f"[OPTIMISTIC REPAIR CASCADED] {tx_id} IN {origin_batch_id} aborted, Transaction {next_tx_id} should be repaired pessimistically.")
                        self.optimistic_state_per_transaction[next_tx_id].need_pessimistic_repair = True
                if self.pessimistic_state_per_batch[origin_batch_id].pessimistic_repair_ready[tx_id]:
                    finished_txs_and_state = [(origin_batch_id, tx_id, state)]
            else:
                finished_txs_and_state = [(origin_batch_id, tx_id, state)]
   
        while finished_txs_and_state:
            batch_id, tx_id, state = finished_txs_and_state.pop(0)
            #log_message(f"[TX FINISH] batch_id: {batch_id}, tx_id: {tx_id}, state: {state}, batch_finished: {self.tx_finished_table_per_batch[batch_id]['finished']}/{self.tx_finished_table_per_batch[batch_id]['total']}")
            self.tx_finished_table_per_batch[batch_id]["finished"] += 1
            batch_finished = (self.tx_finished_table_per_batch[batch_id]["total"] == self.tx_finished_table_per_batch[batch_id]["finished"])
            ready_successor_tx_pessi = self.reminder_successor_tx_pessi(batch_id, tx_id, batch_finished)
            if state == ABORTED:
                tasks_cascaded_repair.setdefault(batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['aborted_txs'].append(tx_id)
            if not PESSIMISTIC_REPAIR:
                for next_batch_id, tx_ids in ready_successor_tx_pessi.items():
                    for pessi_ready_tx in tx_ids:      
                        optimistic_state = self.optimistic_state_per_transaction[pessi_ready_tx]
                        #log_message(f"[AFTER OPT REPAIRED] {pessi_ready_tx} in {next_batch_id} is pessi_ready, its opt repair state:{optimistic_state.optimistic_repair_state}, need_pessimistic_repair: {optimistic_state.need_pessimistic_repair}")
                        if optimistic_state.need_pessimistic_repair:
                            #log_message(f"[PESSIMISTIC REPAIR] {pessi_ready_tx} in {next_batch_id} SEND TO pessi repair")
                            tasks_cascaded_repair.setdefault(next_batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['pessi_repair_txs'].append(pessi_ready_tx)
                        elif optimistic_state.optimistic_repair_state != WAITING:
                            #log_message(f"[OPTIMISTIC REPAIR FINISH] {pessi_ready_tx} in {next_batch_id} is repaired AND don't need pessi, its state: {optimistic_state.optimistic_repair_state}")
                            finished_txs_and_state.append((next_batch_id, pessi_ready_tx, optimistic_state.optimistic_repair_state))
            else:
                for next_batch_id, tx_ids in ready_successor_tx_pessi.items():
                    #log_message(f"[PESSIMISTIC REPAIR] {tx_ids} in {next_batch_id} SEND TO pessi repair")
                    tasks_cascaded_repair.setdefault(next_batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['pessi_repair_txs'].extend(tx_ids)

            if batch_finished:
                tasks_cascaded_repair.setdefault(batch_id, {'batch_finished':False, 'pessi_repair_txs':[], 'aborted_txs':[]})['batch_finished'] = True
                tasks_tx_finish_repair[batch_id] = tasks_cascaded_repair.pop(batch_id)
                self.tx_finished_table_per_batch.pop(batch_id, None)
                self.pessimistic_state_per_batch.pop(batch_id, None)
        if skip_repair:
            tasks_tx_finish_repair.pop(origin_batch_id, None)
        #log_message(f"[REPAIR FINISH] tasks_tx_finish_repair: {tasks_tx_finish_repair}, tasks_cascaded_repair: {tasks_cascaded_repair}")
        return tasks_tx_finish_repair, tasks_cascaded_repair

    
class TransactionSink:
    def __init__(self, workflow_name, batch_size, host_addr):
        # 使用 gevent.queue.Queue 替代列表
        self.queue = gevent.queue.Queue(maxsize=1000)  # 设置最大队列大小，避免内存无限增长
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        # 移除 queue_lock，因为 gevent.queue.Queue 是线程安全的
        self.batch_size = batch_size
        self.repairing_batch_state:RepairingBatchState = RepairingBatchState(workflow_name) 
        self.last_batch_time = time.time()

    def init_batch_processor(self):
        """初始化批处理器，类似 function_manager 的 init 方法"""
        # 立即启动第一次批处理检查
        gevent.spawn_later(VALIDATE_INTERVAL, self._batch_processor_loop)
        # log_message(f"[BATCH PROCESSOR INIT] workflow: {self.workflow_name}, interval: {VALIDATE_INTERVAL}s")

    def validate_batch_check(self):
        """检查队列并进行批处理验证，无论是否达到 batch_size"""
        queue_size = self.queue.qsize()
        current_time = time.time()
        time_since_last_batch = current_time - self.last_batch_time
        if queue_size == 0:
            # 队列为空，直接返回
            return
        # 检查是否满足发送条件
        # 1. 队列中的请求数量达到 batch_size
        # 2. 距离上次发送超过了超时时间，并且队列不为空
        if queue_size < self.batch_size and time_since_last_batch < BATCH_TIMEOUT:
            return
        
        # 确定本次处理的事务数量
        batch_count = min(queue_size, self.batch_size)
        
       # log_message(f"[BATCH CHECK] workflow: {self.workflow_name}, queue_size: {queue_size}, processing: {batch_count}")
        
        # 收集事务
        batch = []
        first_run_finish_time = time.time()
        
        for _ in range(batch_count):
            try:
                transaction = self.queue.get_nowait()
                batch.append(transaction)
            except gevent.queue.Empty:
                # 队列为空，退出收集
                break
        
        # 如果收集到了事务，进行批处理
        if batch:
            self.process_batch(batch, first_run_finish_time)
            self.last_batch_time = current_time
            


    def process_batch(self, batch, first_run_finish_time):
  
        # 转换批次格式
        transformed_batch = self.transform_batch(batch)
        
        # 注册批次
        self.repairing_batch_state.register_batch(
            transformed_batch['batch_id'], 
            transformed_batch['transaction_list'],  
            len(batch)
        )
        
        # 发送验证请求
        self.send_validate_request(transformed_batch, first_run_finish_time)
        
        # log_message(f"[PROCESS BATCH] workflow: {self.workflow_name}, batch_id: {transformed_batch['batch_id']}, size: {len(batch)}, queue remaining: {self.queue.qsize()}")
        
   
    def append(self, transaction_id: str, read_set: Dict[str, Dict], write_set: Dict[str, int], container_port: Dict[str, str], RYW_subjection:Dict[str, dict]):
        """将事务添加到队列中"""
        transaction_data = {
            'transaction_id': transaction_id,
            'read_set': read_set, 
            'write_set': write_set, 
            'container_port': container_port, 
            'RYW_subjection': RYW_subjection
        }
        
        try:
            # 使用非阻塞的方式添加到队列
            self.queue.put_nowait(transaction_data)
            #log_message(f"[APPEND] workflow: {self.workflow_name}, transaction_id: {transaction_id}, queue size: {self.queue.qsize()}")
        except gevent.queue.Full:
            # 如果队列满了，使用阻塞方式等待
            log_message(f"[QUEUE FULL] workflow: {self.workflow_name}, waiting to append transaction: {transaction_id}")
            self.queue.put(transaction_data)
            log_message(f"[APPEND DELAYED] workflow: {self.workflow_name}, transaction_id: {transaction_id}, queue size: {self.queue.qsize()}")

    # transform the batch from a list of txs to a dict, for the convenience of validation.
    # readset and writeset are lists for locking in sequence, so they are not transformed.
    def transform_batch(self, batch):
        transformed_batch = {
            "batch_id": batch[0]["transaction_id"],
            "read_set": {},
            "write_set": {},
            "RYW_subjection": {},
            "container_port": {},
            "transaction_list":[]
        }

        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["read_set"][tx_id]= tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["RYW_subjection"][tx_id] = tx["RYW_subjection"]
            transformed_batch["container_port"][tx_id] = tx["container_port"]
            transformed_batch["transaction_list"].append(tx_id)
        return transformed_batch

    def validate_batch(self):
        """保留旧的 validate_batch 方法以保持兼容性（已弃用）"""
        log_message(f"[DEPRECATED] validate_batch() called directly for workflow: {self.workflow_name}")
        self.validate_batch_check()


    def fin_repair_or_abort(self, batch_id, transaction_id, repair_mode, state, skip_repair):
        tasks_tx_finish_repair, tasks_cascaded_repair = self.repairing_batch_state.after_transaction_finish(batch_id, repair_mode, transaction_id, state, skip_repair)
        if tasks_tx_finish_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_tx_finish_repair)]
            gevent.joinall(tasks)
        if tasks_cascaded_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_cascaded_repair)]
            gevent.joinall(tasks)

    def clear_opt_table_after_finish(self, batch_list):
        self.repairing_batch_state.clear_opt_table_after_finish(batch_list)

    # called only in pessimistic repair, to update the subjection info of the batch.
    def register_repair_info_after_validate(self, batch_id, batch_sub, tx_sub, sub_per_tx):
        #log_message(f"[PESSIMISTIC REGISTER] batch_id: {batch_id}, batch_sub: {batch_sub}, tx_sub: {tx_sub}, sub_per_tx_optimistic: {sub_per_tx}")
        ready_txs, opt_txs_become_pessi = self.repairing_batch_state.update_subjection_info(batch_id, batch_sub, tx_sub, sub_per_tx)
        return {'ready_txs': ready_txs, 'opt_txs_become_pessi':opt_txs_become_pessi}
    
    def repair_finish_on_validator(self, data):
        remote_url = 'http://{}/fin_repair'.format(VALIDATOR_ADDR)
        requests.post(remote_url, json={'workflow_name':self.workflow_name, 'data':data})

    def send_validate_request(self, batch, first_run_finish_time):
        remote_url = 'http://{}/validate'.format(VALIDATOR_ADDR)
        data = {
            'workflow_name': self.workflow_name,
            "batch": batch,
            "batch_id": batch["batch_id"],
            "first_run_finish_time": first_run_finish_time
        }
        #log_message(f"[VALIDATE] batch_id:{batch['batch_id']}, transaction_list:{batch['transaction_list']}, first_run_finish_time: {first_run_finish_time}")
        requests.post(remote_url, json=data)
        

    def commit_batch(self, batch_id):
        remote_url = 'http://{}/commit'.format(VALIDATOR_ADDR)
        data = {
                'workflow_name': self.workflow_name,
                "batch_id": batch_id
            }
        requests.post(remote_url, json=data)