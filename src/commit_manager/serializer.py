from gevent import monkey
import gevent

monkey.patch_all()
from multiprocessing import Process
import time
import re
import os
import validator_repo
from datetime import datetime
import logging

repo = validator_repo.Repository()
VALIDATE = 1
COMMIT = 3
CASCADED_COMMIT = 4

log_file = '../../logging/serializer.log'

# 删除旧的日志文件（如果存在）
if os.path.exists(log_file):
    os.remove(log_file)

# 配置logging模块


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

class SerializerProcess(Process):
    def __init__(self, workflow_name, req_queue, result_pipes, handler_task_queues, function_pos):
        super().__init__()
        self.workflow_name = workflow_name
        self.req_queue = req_queue
        self.result_pipes = result_pipes 
        self.logger = self.setup_logger()
        self.handler_task_queues = handler_task_queues  # {handler_id: task_queue}, used to trigger seq commits  
        self.key_version_table = repo.get_initial_global_table() # {key: version}                                                 # {key: [{'batch_id',xx, 'tx_id':xx, 'func':xx}] }
        # infomation for commiting batches.
        self.function_pos = function_pos  # {func_name: {'ip': ip, 'port': port}}, used to get the ip of the function for commit.
        for func, ip in function_pos.items():
            self.function_pos[func] = extract_ip(ip)  # Extract IP without port

    def setup_logger(self):
        logger = logging.getLogger(f'{self.workflow_name}_serializer')
        logger.setLevel(logging.INFO)
        # 创建文件处理器
        handler = logging.FileHandler(f'../../logging/{self.workflow_name}_serializer.log', mode='a')
        handler.setLevel(logging.INFO)
        
        # 创建格式化器
        formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] %(message)s', 
                                    datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        # 添加处理器到logger
        if not logger.handlers:
            logger.addHandler(handler)
        
        return logger


    def log_message(self, message):
        self.logger.info(message)
        # 强制刷新缓冲区
        for handler in self.logger.handlers:
            handler.flush()
        
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
                version = get_timestamp()
                commitable_keys, expired_set, succeed_txs, abort_txs = self.accessed_set_validate(version, data['transaction_list'], data['read_set'], data['write_set'])
                version = get_timestamp()
                for key in commitable_keys:
                    self.key_version_table[key] = version
                self.result_pipes[handler_id].put((batch_id, (version, commitable_keys, expired_set, succeed_txs, abort_txs)))

    def accessed_set_validate(self, transaction_list, read_set_per_batch, write_set_per_batch):
        batch_write_info = {}
        commitable_keys = {}
        succeed_txs = {}
        abort_txs = {}
        expired_set = {}
        for tx_id in transaction_list:
            rs = read_set_per_batch[tx_id]
            ws = write_set_per_batch[tx_id]
            commitable = self.validate_transaction(rs, batch_write_info, expired_set)
            if not commitable:
                abort_txs[tx_id] = True
            else:
                succeed_txs[tx_id] = True
            for key, func in ws.keys():
                batch_write_info[key] = tx_id
                if commitable:
                    commitable_keys[key] = [tx_id, func]
        return commitable_keys, expired_set, succeed_txs, abort_txs

    def validate_transaction(self, read_set, batch_write_info, expired_set):
        commitable = True
        for func, kv_pairs in read_set.items():
            for key, version in kv_pairs.items():
                if key in batch_write_info:
                    commitable = False
                prev_version = self.key_version_table.get(key, None)
                if prev_version is not None and version < prev_version:
                    expired_set.setdefault(key, {})[func] = True
                    commitable = False
        return commitable

        


