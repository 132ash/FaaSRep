from gevent import monkey
monkey.patch_all()
import gevent
import os
import requests
import logging
from typing import Dict
import sys
import time
import gevent.queue  # 添加 gevent 队列导入
sys.path.append('../../config')
import config

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

class TransactionSink:
    def __init__(self, workflow_name, batch_size, host_addr):
        # 使用 gevent.queue.Queue 替代列表
        self.queue = gevent.queue.Queue(maxsize=1000)  # 设置最大队列大小，避免内存无限增长
        self.host_addr = host_addr
        self.workflow_name = workflow_name
        # 移除 queue_lock，因为 gevent.queue.Queue 是线程安全的
        self.batch_size = batch_size
        self.last_batch_time = time.time()

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
        # 发送验证请求
        self.send_validate_request(transformed_batch, first_run_finish_time)
        # log_message(f"[PROCESS BATCH] workflow: {self.workflow_name}, batch_id: {transformed_batch['batch_id']}, size: {len(batch)}, queue remaining: {self.queue.qsize()}")
        
   
    def append(self, transaction_id: str, read_set: Dict[str, Dict], write_set: Dict[str, int]):
        """将事务添加到队列中"""
        transaction_data = {
            'transaction_id': transaction_id,
            'read_set': read_set, 
            'write_set': write_set
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
            "transaction_list":[]
        }

        for tx in batch:
            tx_id = tx["transaction_id"]
            transformed_batch["read_set"][tx_id]= tx["read_set"]
            transformed_batch["write_set"][tx_id]=tx["write_set"]
            transformed_batch["transaction_list"].append(tx_id)
        return transformed_batch

    def validate_batch(self):
        """保留旧的 validate_batch 方法以保持兼容性（已弃用）"""
        log_message(f"[DEPRECATED] validate_batch() called directly for workflow: {self.workflow_name}")
        self.validate_batch_check()

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