from gevent import monkey
import gevent

monkey.patch_all()
from multiprocessing import Process
from collections import deque
import time
import re
import validator_repo
from datetime import datetime
import logging
import sys
from serializer_state import (
    BatchCommitTracker,
    DependencyBuilder,
    GlobalVersionTable,
    WriterIndex,
    normalize_commit_keys,
)

sys.path.append('../../config')
import config
from logging_utils import RunAwareFileHandler

repo = validator_repo.Repository()
VALIDATE = 1
COMMIT = 3
CASCADED_COMMIT = 4

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
        self.version_table = GlobalVersionTable(repo.get_initial_global_table())
        self.writer_index = WriterIndex()
        self.commit_tracker = BatchCommitTracker()
        self.dependency_builder = DependencyBuilder(
            self.version_table,
            self.writer_index,
            self.commit_tracker,
        )
        # Legacy attribute names are kept for debugging callers that inspect the
        # serializer instance directly.
        self.key_version_table = self.version_table.versions # {key: version}
        self.key_writers = self.writer_index._writers   # {key: deque[(batch_id, tx_id, func)]}
        self.batch_write_info = self.commit_tracker.batch_write_info
        self.commit_keys_per_batch = {}
        self.batch_validator_assignment = self.commit_tracker.batch_validator_assignment
        self.commit_suspended_batches = self.commit_tracker.commit_suspended_batches
        self.function_pos = function_pos  # {func_name: {'ip': ip, 'port': port}}, used to get the ip of the function for commit.
        for func, ip in function_pos.items():
            self.function_pos[func] = extract_ip(ip)  # Extract IP without port

    def setup_logger(self):
        logger = logging.getLogger(f'{self.workflow_name}_serializer')
        logger.setLevel(logging.INFO)
        # 动态跟随当前 run_id 的文件处理器
        handler = RunAwareFileHandler(config.ROOT_DIR, f'{self.workflow_name}_serializer.log')
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
                msg = self.req_queue.get_nowait()
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
                expired_set, subjection_set, pessi_sink_info = self.accessed_set_validate(batch_id, version, data['transaction_list'], data['read_set'], data['write_set'])
                self.logger.info(f"[VALIDATE] {batch_id}: expired_set={expired_set}, subjection_set={subjection_set}, pessi_sink_info={pessi_sink_info}")
                # if not batch_need_repair:
                #     self.commit_keys_per_batch[batch_id] = self.batch_write_info[batch_id]['writes'].copy()  # commit keys for this batch.
                #     commit_list_for_current_handler, commit_keys_on_worker = self.commit_all_ready_batches(handler_id, batch_id)
                self.result_pipes[handler_id].put((batch_id, (expired_set, subjection_set, pessi_sink_info)))
                
            elif op == COMMIT:
                self.commit_keys_per_batch[batch_id] = data['commit_keys']
                commit_list_for_current_handler, commit_keys_on_worker = self.commit_all_ready_batches(handler_id, batch_id)
                self.result_pipes[handler_id].put((batch_id, (commit_list_for_current_handler, commit_keys_on_worker)))

    # check if this batch is ready to commit.
    # if not, suspend this batch, and wait for its ancestors to finish.
    # in pessimistic mode, the batch is ready for sure: only flush the ready writes.
    def commit_all_ready_batches(self, current_handler_id, current_batch_id):
        ready, commit_list_per_handler, commit_keys_on_worker = self.get_commitable_batches(current_batch_id)
        self.logger.info(f"[COMMIT] {current_batch_id} by handler {current_handler_id}: ready={ready}, commit_list_per_handler={commit_list_per_handler}, commit_keys_on_worker={commit_keys_on_worker}")
        commit_list_for_current_handler = commit_list_per_handler.pop(current_handler_id, [])
        if ready:
            for handler_id, commit_batch_list in commit_list_per_handler.items():
                self.handler_task_queues[handler_id].put(('', CASCADED_COMMIT, commit_batch_list))
        else:
            self.commit_suspended_batches[current_batch_id] = current_handler_id 
        return commit_list_for_current_handler, commit_keys_on_worker


    def accessed_set_validate(self, batch_id,version, transaction_list, read_set_per_batch, write_set_per_batch):
        result = self.dependency_builder.validate_batch(
            handler_id=self.batch_validator_assignment[batch_id],
            batch_id=batch_id,
            version=version,
            transaction_list=transaction_list,
            read_set_per_batch=read_set_per_batch,
            write_set_per_batch=write_set_per_batch,
        )
        return result.to_legacy()

    def get_expired_set_and_subjection(self,batch_id, tx_id, expired_set, subjection_set, read_set, pessi_sink_info, tx_index_inside_batch:dict):
        raise RuntimeError("SerializerProcess.get_expired_set_and_subjection is replaced by DependencyBuilder")

    def update_key_writers(self, batch_id, tx_id, write_set):
        self.dependency_builder._record_writes(batch_id, tx_id, write_set)

    def prev_batch_committed(self, batch_id):
        # check if this batch is ready to commit.
        return self.commit_tracker.is_ready(batch_id)

    def get_commitable_batches(self, target_batch_id):
        if not self.prev_batch_committed(target_batch_id):
            self.logger.info(f"[COMMIT] Batch {target_batch_id} is not ready to commit, waiting for ancestors to finish.")
            return False, {}, {}
        batches_ready_for_committing = deque([target_batch_id])
        commit_keys_on_worker = {} # {key: [(tx_id, func, version)]}
        commit_list_per_handler = {}  # {handler_id: [batch_id]}

        while batches_ready_for_committing:
            # add the first ready batch to commit list.
            current_batch_id = batches_ready_for_committing.popleft()
            current_batch_write_info, current_handler_id = self.commit_tracker.pop_batch(current_batch_id)
            current_batch_commit_keys = self.commit_keys_per_batch.pop(current_batch_id)
            current_commit_keys = normalize_commit_keys(current_batch_commit_keys)
            version =  current_batch_write_info.version
            # check cascaded batches: the writes are all ready.
            for key in current_batch_write_info.writes:
                self.logger.info(f"[COMMIT] {current_batch_id} commit {key}:writers {self.key_writers.get(key, [])}")
                writer = self.writer_index.pop_committed_writer(key)
                if writer is None:
                    continue
                if key in current_commit_keys:
                    self.version_table.mark_committed(key, version)
                    writer_tx_id, writer_func = writer.tx_id, writer.func
                    commit_keys_on_worker[key] = (writer_tx_id, writer_func, version)
                next_writer = self.writer_index.first_writer(key)
                if next_writer is not None:
                    cascaded_batch_id = next_writer.batch_id
                    became_ready = self.commit_tracker.mark_key_unblocked(cascaded_batch_id)
                    # only suspended batches are ready to commit cascaded.
                    if became_ready and self.commit_tracker.pop_suspended_if_ready(cascaded_batch_id):
                        batches_ready_for_committing.append(cascaded_batch_id)
            commit_list_per_handler.setdefault(current_handler_id, []).append(current_batch_id)
        return True, commit_list_per_handler, commit_keys_on_worker


        
