from gevent import monkey
monkey.patch_all()
import gevent
import requests
import random
import logging
from collections import deque
from typing import Dict, Iterable, Set
import sys
import time
from batch_state_struct import BatchRepairState, SinkCommand, TransactionRepairState
import gevent.lock
import gevent.queue  # 添加 gevent 队列导入
sys.path.append('../../config')
import config
from logging_utils import RunAwareFileHandler

REPAIRED = config.REPAIRED
ABORTED = config.ABORTED    
WAITING = config.RUNNING
OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

ABORT_PROB = config.ABORT_PROB

PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR
VALIDATOR_ADDR = config.VALIDATOR_ADDR
VALIDATE_INTERVAL = config.VALIDATE_INTERVAL
BATCH_TIMEOUT = config.BATCH_TIMEOUT # 50ms

def setup_logger():
    logger = logging.getLogger('sink')
    logger.setLevel(logging.INFO)
    # 动态跟随当前 run_id 的文件处理器
    file_handler = RunAwareFileHandler(config.ROOT_DIR, 'sink.log')
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
        self.batch_state_per_batch: Dict[str, BatchRepairState] = {}
        self.transaction_state_per_tx: Dict[str, TransactionRepairState] = {}
        # Legacy aliases kept for callers/tests that still inspect these names.
        self.pessimistic_state_per_batch = self.batch_state_per_batch
        self.optimistic_state_per_transaction = self.transaction_state_per_tx
        self.transaction_list_per_batch: Dict[str, list] = {}
        self.tx_finished_table_per_batch: Dict[str, dict] = {}
        self.completed_batches: Set[str] = set()
        self.committed_batches: Set[str] = set()
        self.aborted_transactions: Set[str] = set()
        self.unknown_predecessors: Dict[str, Set[str]] = {}
        self.batch_registered_at: Dict[str, float] = {}
        self.state_lock  = gevent.lock.BoundedSemaphore()
        self.workflow_name = workflow_name

    def register_batch(self, batch_id, tx_list, batch_size):
        self.state_lock.acquire()
        try:
            self.transaction_list_per_batch[batch_id] = list(tx_list)
            self.batch_registered_at[batch_id] = time.time()
            self.tx_finished_table_per_batch[batch_id] = {'total':batch_size, "finished": 0}
            self.batch_state_per_batch[batch_id] = BatchRepairState(batch_id, list(tx_list))
            for tx_id in tx_list:
                self.transaction_state_per_tx[tx_id] = TransactionRepairState(tx_id, batch_id)
        finally:
            self.state_lock.release()
        log_message(f"[REPAIR REGISTER] tx_finished_table_per_batch: {self.tx_finished_table_per_batch}")

    def _batch_progress_snapshot(self, batch_id: str):
        tx_order = list(self.transaction_list_per_batch.get(batch_id, []))
        if not tx_order:
            return None
        batch_state = self.batch_state_per_batch.get(batch_id)
        tx_states = []
        pending_count = 0
        for tx_id in tx_order:
            tx_state = self.transaction_state_per_tx.get(tx_id)
            if tx_state is None:
                continue
            if tx_state.final_state is None:
                pending_count += 1
            tx_states.append({
                "tx": tx_id,
                "opt": tx_state.optimistic_state,
                "needs_pessi": tx_state.needs_pessimistic,
                "pessi_ready": tx_state.pessimistic_ready,
                "pessi_running": tx_state.pessimistic_running,
                "final": tx_state.final_state,
                "reasons": sorted(tx_state.missing_predecessors),
            })
        finished = self.tx_finished_table_per_batch.get(batch_id, {}).get("finished", 0)
        total = self.tx_finished_table_per_batch.get(batch_id, {}).get("total", len(tx_order))
        age_s = time.time() - self.batch_registered_at.get(batch_id, time.time())
        return {
            "batch_id": batch_id,
            "finished": finished,
            "total": total,
            "pending_count": pending_count,
            "age_s": age_s,
            "ready_queue": sorted(batch_state.ready_pessi_queue) if batch_state is not None else [],
            "tx_states": tx_states,
        }

    def _log_batch_progress(self, batch_ids: Iterable[str], tag: str) -> None:
        seen = set()
        for batch_id in batch_ids:
            if not batch_id or batch_id in seen:
                continue
            seen.add(batch_id)
            snapshot = self._batch_progress_snapshot(batch_id)
            if snapshot is None:
                continue
            log_message(
                f"[{tag}] workflow={self.workflow_name}, batch={batch_id}, "
                f"finished={snapshot['finished']}/{snapshot['total']}, "
                f"pending={snapshot['pending_count']}, age={snapshot['age_s']:.2f}s, "
                f"ready_queue={snapshot['ready_queue']}, tx_states={snapshot['tx_states']}"
            )

    def pending_batch_snapshots(self, idle_after: float):
        now = time.time()
        snapshots = []
        for batch_id, registered_at in self.batch_registered_at.items():
            snapshot = self._batch_progress_snapshot(batch_id)
            if snapshot is None or snapshot["pending_count"] == 0:
                continue
            if now - registered_at < idle_after:
                continue
            snapshots.append(snapshot)
        return snapshots

    def update_subjection_info(self, batch_id:str, batch_sub, tx_sub, sub_per_tx_optimistic=None):
        """
        Update the subjection info for the given batch. 
        batch_sub: {prev_batch_id:[txs]}
        tx_sub: {prev_batch_id: [txs]}
        """
        sub_per_tx_optimistic = sub_per_tx_optimistic or {}
        self.state_lock.acquire()
        try:
            batch_state = self.batch_state_per_batch.get(batch_id)
            if batch_state is None:
                self._record_unknown(batch_id, "register_dependencies_for_missing_batch")
                return {}, {}
            log_message(
                f"[SINK PESSI REGISTER] workflow={self.workflow_name}, batch={batch_id}, "
                f"batch_dep_cnt={len(batch_sub)}, tx_dep_cnt={len(tx_sub)}, opt_dep_cnt={len(sub_per_tx_optimistic)}"
            )

            opt_txs_become_pessi = {}
            for prev_batch_id, next_txs in batch_sub.items():
                next_tx_set = set(next_txs)
                prev_batch_info = self.batch_state_per_batch.get(prev_batch_id)
                if prev_batch_info is not None:
                    prev_batch_info.add_successor_batch(batch_id, next_tx_set)
                    batch_state.add_batch_dependency(next_tx_set, predecessor=prev_batch_id)
                elif prev_batch_id in self.committed_batches or prev_batch_id in self.completed_batches:
                    continue
                else:
                    self._record_unknown(batch_id, f"unknown_batch:{prev_batch_id}")
                    for tx_id in next_tx_set:
                        self._mark_needs_pessimistic(tx_id, f"unknown_batch:{prev_batch_id}")
                        opt_txs_become_pessi[tx_id] = True

            normalized_tx_sub = {
                prev_tx_id: set(next_txs)
                for prev_tx_id, next_txs in tx_sub.items()
            }
            for prev_tx_id, next_txs in list(normalized_tx_sub.items()):
                if prev_tx_id in self.aborted_transactions:
                    for tx_id in next_txs:
                        self._mark_needs_pessimistic(tx_id, f"aborted_tx:{prev_tx_id}")
                        opt_txs_become_pessi[tx_id] = True
                    normalized_tx_sub.pop(prev_tx_id, None)
            batch_state.add_tx_dependencies(normalized_tx_sub)

            ready_txs = batch_state.mark_initial_ready()
            for ready_tx in ready_txs:
                tx_state = self.transaction_state_per_tx.get(ready_tx)
                if tx_state is not None:
                    tx_state.pessimistic_ready = True

            # update optimistic subjection
            for prev_transaction_id, next_txs in sub_per_tx_optimistic.items():
                next_tx_set = set(next_txs.keys() if isinstance(next_txs, dict) else next_txs)
                prev_tx_state = self.transaction_state_per_tx.get(prev_transaction_id)
                if prev_tx_state is None:
                    self._record_unknown(batch_id, f"unknown_tx:{prev_transaction_id}")
                    for next_tx in next_tx_set:
                        self._mark_needs_pessimistic(next_tx, f"unknown_tx:{prev_transaction_id}")
                        opt_txs_become_pessi[next_tx] = True
                    continue
                prev_tx_state.add_optimistic_successors(next_tx_set)
                if prev_tx_state.final_state == ABORTED or prev_tx_state.optimistic_state == ABORTED:
                    for next_tx in next_tx_set:
                        self._mark_needs_pessimistic(next_tx, f"aborted_tx:{prev_transaction_id}")
                        opt_txs_become_pessi[next_tx] = True
            log_message(f"[CHECK BATCH PESSI SUB] In batch_id {batch_id}, {opt_txs_become_pessi} needs pessi repair, {list(ready_txs)} is pessi ready.")
            log_message(
                f"[SINK PESSI REGISTER DONE] workflow={self.workflow_name}, batch={batch_id}, "
                f"ready={sorted(ready_txs)}, needs_pessi={sorted(opt_txs_become_pessi.keys())}"
            )
            self._log_batch_progress([batch_id], "SINK PENDING")
            return {tx_id: True for tx_id in ready_txs}, opt_txs_become_pessi
        finally:
            self.state_lock.release()

    def clear_opt_table_after_finish(self, batch_id_list):
        self.state_lock.acquire()
        try:
            tasks_tx_finish_repair = {}
            tasks_cascaded_repair = {}
            finished_txs_and_state = deque()
            for batch_id in batch_id_list:
                batch_state = self.batch_state_per_batch.get(batch_id)
                if batch_state is not None:
                    ready_successor_tx_pessi = batch_state.release_successors_after_batch_finish()
                    if ready_successor_tx_pessi:
                        log_message(
                            f"[SINK COMMIT RELEASE] workflow={self.workflow_name}, batch={batch_id}, "
                            f"released={ready_successor_tx_pessi}"
                        )
                    self._handle_ready_successors(
                        ready_successor_tx_pessi,
                        finished_txs_and_state,
                        tasks_cascaded_repair,
                    )
                self.committed_batches.add(batch_id)
                self.completed_batches.add(batch_id)
                for tx_id in self.transaction_list_per_batch.get(batch_id, []):
                    self.transaction_state_per_tx.pop(tx_id, None)
                self.batch_state_per_batch.pop(batch_id, None)
                self.transaction_list_per_batch.pop(batch_id, None)
                self.tx_finished_table_per_batch.pop(batch_id, None)
                self.batch_registered_at.pop(batch_id, None)

            while finished_txs_and_state:
                next_batch_id, finished_tx_id, finished_state = finished_txs_and_state.popleft()
                self._record_final_resolution(
                    next_batch_id,
                    finished_tx_id,
                    finished_state,
                    finished_txs_and_state,
                    tasks_cascaded_repair,
                    tasks_tx_finish_repair,
                )
        finally:
            self.state_lock.release()
        return self._legacy_tasks(tasks_tx_finish_repair), self._legacy_tasks(tasks_cascaded_repair)
       
    def after_transaction_finish(self, origin_batch_id, repair_mode, tx_id, state, skip_repair):
        self.state_lock.acquire()
        try:
            log_message(
                f"[SINK FINISH EVENT] workflow={self.workflow_name}, batch={origin_batch_id}, tx={tx_id}, "
                f"mode={repair_mode}, state={state}, skip_repair={skip_repair}"
            )
            tasks_tx_finish_repair = {}
            tasks_cascaded_repair = {}
            finished_txs_and_state = deque()

            if skip_repair:
                for skipped_tx in self.transaction_list_per_batch.get(origin_batch_id, []):
                    finished_txs_and_state.append((origin_batch_id, skipped_tx, state))
            elif PESSIMISTIC_REPAIR:
                finished_txs_and_state.append((origin_batch_id, tx_id, state))
            else:
                self._handle_repair_finish(
                    origin_batch_id,
                    repair_mode,
                    tx_id,
                    state,
                    finished_txs_and_state,
                    tasks_cascaded_repair,
                )

            while finished_txs_and_state:
                batch_id, finished_tx_id, finished_state = finished_txs_and_state.popleft()
                self._record_final_resolution(
                    batch_id,
                    finished_tx_id,
                    finished_state,
                    finished_txs_and_state,
                    tasks_cascaded_repair,
                    tasks_tx_finish_repair,
                )

            if skip_repair:
                tasks_tx_finish_repair.pop(origin_batch_id, None)
            log_message(f"[REPAIR FINISH] tasks_tx_finish_repair: {tasks_tx_finish_repair}, tasks_cascaded_repair: {tasks_cascaded_repair}")
            if tasks_tx_finish_repair or tasks_cascaded_repair:
                log_message(
                    f"[SINK FINISH TASKS] workflow={self.workflow_name}, batch={origin_batch_id}, "
                    f"tx_finish_batches={list(tasks_tx_finish_repair.keys())}, "
                    f"cascaded_batches={list(tasks_cascaded_repair.keys())}"
                )
            batches_to_log = [origin_batch_id]
            batches_to_log.extend(tasks_tx_finish_repair.keys())
            batches_to_log.extend(tasks_cascaded_repair.keys())
            self._log_batch_progress(batches_to_log, "SINK PENDING")
            return self._legacy_tasks(tasks_tx_finish_repair), self._legacy_tasks(tasks_cascaded_repair)
        finally:
            self.state_lock.release()

    def _handle_repair_finish(self, batch_id, repair_mode, tx_id, state, finished_txs_and_state, tasks):
        tx_state = self.transaction_state_per_tx.get(tx_id)
        if tx_state is None:
            self._record_unknown(batch_id, f"finish_for_unknown_tx:{tx_id}")
            return
        if tx_state.final_state is not None:
            return

        if repair_mode == OPT_REPAIR:
            if tx_state.needs_pessimistic:
                tx_state.optimistic_result_rejected = True
                log_message(
                    f"[SINK OPT RESULT DROPPED] workflow={self.workflow_name}, batch={batch_id}, tx={tx_id}, "
                    f"reasons={sorted(tx_state.missing_predecessors)}"
                )
                if tx_state.can_trigger_pessimistic():
                    self._schedule_pessimistic(tasks, tx_state.batch_id, tx_id)
                return
            if random.random() < ABORT_PROB:
                state = ABORTED
            tx_state.optimistic_state = state
            if state == ABORTED:
                self.aborted_transactions.add(tx_id)
                for next_tx_id in tx_state.optimistic_successors:
                    self._mark_needs_pessimistic(next_tx_id, f"aborted_tx:{tx_id}", tasks)
            if tx_state.pessimistic_ready:
                finished_txs_and_state.append((batch_id, tx_id, state))
            return

        if repair_mode == PESSI_REPAIR:
            tx_state.pessimistic_running = False
            log_message(
                f"[SINK PESSI FINISHED] workflow={self.workflow_name}, batch={batch_id}, tx={tx_id}, state={state}"
            )
            finished_txs_and_state.append((batch_id, tx_id, state))

    def _record_final_resolution(
        self,
        batch_id,
        tx_id,
        state,
        finished_txs_and_state,
        tasks_cascaded_repair,
        tasks_tx_finish_repair,
    ):
        tx_state = self.transaction_state_per_tx.get(tx_id)
        batch_state = self.batch_state_per_batch.get(batch_id)
        if tx_state is None or tx_state.final_state is not None:
            return
        tx_state.final_state = state
        tx_state.finish_recorded = True
        if state == ABORTED:
            self.aborted_transactions.add(tx_id)
            self._task(tasks_cascaded_repair, batch_id).aborted_txs.add(tx_id)

        if batch_id in self.tx_finished_table_per_batch:
            self.tx_finished_table_per_batch[batch_id]["finished"] += 1
        if batch_state is None:
            return

        batch_state.finished_count += 1
        batch_finished = batch_state.finished_count == batch_state.batch_size
        if batch_finished:
            batch_state.batch_finished = True
            ready_successor_tx_pessi = {}
        else:
            ready_successor_tx_pessi = batch_state.release_transactions_after_tx_finish(tx_id)

        self._handle_ready_successors(
            ready_successor_tx_pessi,
            finished_txs_and_state,
            tasks_cascaded_repair,
            ready_already_marked=not batch_finished,
        )

        if batch_finished:
            task = self._task(tasks_cascaded_repair, batch_id)
            task.batch_finished = True
            tasks_tx_finish_repair[batch_id] = tasks_cascaded_repair.pop(batch_id)

    def _handle_ready_successors(self, ready_successor_tx_pessi, finished_txs_and_state, tasks, ready_already_marked=False):
        for next_batch_id, tx_ids in ready_successor_tx_pessi.items():
            next_batch = self.batch_state_per_batch.get(next_batch_id)
            if ready_already_marked:
                ready_ids = set(tx_ids)
            else:
                ready_ids = next_batch.mark_tx_ready(tx_ids) if next_batch else set(tx_ids)
            for ready_tx_id in ready_ids:
                tx_state = self.transaction_state_per_tx.get(ready_tx_id)
                if tx_state is None:
                    self._record_unknown(next_batch_id, f"ready_unknown_tx:{ready_tx_id}")
                    continue
                tx_state.pessimistic_ready = True
                if PESSIMISTIC_REPAIR:
                    self._schedule_pessimistic(tasks, next_batch_id, ready_tx_id)
                elif tx_state.needs_pessimistic:
                    self._schedule_pessimistic(tasks, next_batch_id, ready_tx_id)
                elif tx_state.optimistic_state != WAITING:
                    finished_txs_and_state.append(
                        (next_batch_id, ready_tx_id, tx_state.optimistic_state)
                    )

    def _mark_needs_pessimistic(self, tx_id, reason, tasks=None):
        tx_state = self.transaction_state_per_tx.get(tx_id)
        if tx_state is None:
            self._record_unknown("", f"{reason}->missing_successor:{tx_id}")
            return
        if tx_state.final_state is not None:
            return
        tx_state.mark_needs_pessimistic(reason)
        log_message(
            f"[SINK MARK PESSI] workflow={self.workflow_name}, batch={tx_state.batch_id}, tx={tx_id}, "
            f"reason={reason}, ready={tx_state.pessimistic_ready}, running={tx_state.pessimistic_running}"
        )
        if tasks is not None and tx_state.can_trigger_pessimistic():
            self._schedule_pessimistic(tasks, tx_state.batch_id, tx_id)

    def _schedule_pessimistic(self, tasks, batch_id, tx_id):
        tx_state = self.transaction_state_per_tx.get(tx_id)
        if tx_state is None or tx_state.final_state is not None:
            return
        if tx_state.pessimistic_running:
            return
        tx_state.pessimistic_running = True
        log_message(
            f"[SINK PESSI SCHEDULED] workflow={self.workflow_name}, batch={batch_id}, tx={tx_id}, "
            f"reasons={sorted(tx_state.missing_predecessors)}"
        )
        self._task(tasks, batch_id).pessi_repair_txs.add(tx_id)

    def _record_unknown(self, batch_id, reason):
        self.unknown_predecessors.setdefault(batch_id, set()).add(reason)
        log_message(f"[SINK WARNING] workflow={self.workflow_name}, batch={batch_id}, reason={reason}")

    def _task(self, tasks, batch_id):
        return tasks.setdefault(batch_id, SinkCommand())

    def _legacy_tasks(self, tasks):
        legacy = {}
        for batch_id, task in tasks.items():
            legacy[batch_id] = {
                'batch_finished': task.batch_finished,
                'pessi_repair_txs': self._ordered_txs(batch_id, task.pessi_repair_txs),
                'aborted_txs': self._ordered_txs(batch_id, task.aborted_txs),
            }
        return legacy

    def _ordered_txs(self, batch_id, tx_ids: Iterable[str]):
        tx_set = set(tx_ids)
        order = self.transaction_list_per_batch.get(batch_id, [])
        ordered = [tx_id for tx_id in order if tx_id in tx_set]
        ordered.extend(sorted(tx_set.difference(ordered)))
        return ordered

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
        self.last_pending_report_at = 0.0

    def init_batch_processor(self):
        """初始化批处理器，类似 function_manager 的 init 方法"""
        # 立即启动第一次批处理检查
        gevent.spawn_later(VALIDATE_INTERVAL, self._batch_processor_loop)
        log_message(f"[BATCH PROCESSOR INIT] workflow: {self.workflow_name}, interval: {VALIDATE_INTERVAL}s")

    def _batch_processor_loop(self):
        gevent.spawn_later(VALIDATE_INTERVAL, self._batch_processor_loop)
        gevent.spawn(self.validate_batch_check)

    def validate_batch_check(self):
        """检查队列并进行批处理验证，无论是否达到 batch_size"""
        self.log_stuck_repairs_if_needed()
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
        
        log_message(f"[BATCH CHECK] workflow: {self.workflow_name}, queue_size: {queue_size}, processing: {batch_count}")
        
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

    def log_stuck_repairs_if_needed(self):
        now = time.time()
        if now - self.last_pending_report_at < 5.0:
            return
        self.last_pending_report_at = now
        snapshots = self.repairing_batch_state.pending_batch_snapshots(idle_after=5.0)
        for snapshot in snapshots[:10]:
            log_message(
                f"[SINK STUCK] workflow={self.workflow_name}, batch={snapshot['batch_id']}, "
                f"finished={snapshot['finished']}/{snapshot['total']}, pending={snapshot['pending_count']}, "
                f"age={snapshot['age_s']:.2f}s, ready_queue={snapshot['ready_queue']}, "
                f"tx_states={snapshot['tx_states']}"
            )
            


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
        
        log_message(f"[PROCESS BATCH] workflow: {self.workflow_name}, batch_id: {transformed_batch['batch_id']}, size: {len(batch)}, queue remaining: {self.queue.qsize()}")
        
   
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
            log_message(f"[APPEND] workflow: {self.workflow_name}, transaction_id: {transaction_id}, queue size: {self.queue.qsize()}")
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
        log_message(
            f"[SINK INBOUND] workflow={self.workflow_name}, batch={batch_id}, tx={transaction_id}, "
            f"mode={repair_mode}, state={state}, skip_repair={skip_repair}"
        )
        tasks_tx_finish_repair, tasks_cascaded_repair = self.repairing_batch_state.after_transaction_finish(batch_id, repair_mode, transaction_id, state, skip_repair)
        if tasks_tx_finish_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_tx_finish_repair)]
            self._join_and_report(tasks, f"finish repair {batch_id}")
        if tasks_cascaded_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_cascaded_repair)]
            self._join_and_report(tasks, f"cascade repair {batch_id}")

    def clear_opt_table_after_finish(self, batch_list):
        tasks_tx_finish_repair, tasks_cascaded_repair = self.repairing_batch_state.clear_opt_table_after_finish(batch_list)
        if tasks_tx_finish_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_tx_finish_repair)]
            self._join_and_report(tasks, f"commit release finish repair {batch_list}")
        if tasks_cascaded_repair:
            tasks = [gevent.spawn(self.repair_finish_on_validator, tasks_cascaded_repair)]
            self._join_and_report(tasks, f"commit release cascade repair {batch_list}")

    # called only in pessimistic repair, to update the subjection info of the batch.
    def register_repair_info_after_validate(self, batch_id, batch_sub, tx_sub, sub_per_tx):
        log_message(f"[PESSIMISTIC REGISTER] batch_id: {batch_id}, batch_sub: {batch_sub}, tx_sub: {tx_sub}, sub_per_tx_optimistic: {sub_per_tx}")
        ready_txs, opt_txs_become_pessi = self.repairing_batch_state.update_subjection_info(batch_id, batch_sub, tx_sub, sub_per_tx)
        return {'ready_txs': ready_txs, 'opt_txs_become_pessi':opt_txs_become_pessi}
    
    def repair_finish_on_validator(self, data):
        remote_url = 'http://{}/fin_repair'.format(VALIDATOR_ADDR)
        return self._post_json(remote_url, {'workflow_name':self.workflow_name, 'data':data}, "repair finish on validator") is not None

    def send_validate_request(self, batch, first_run_finish_time):
        remote_url = 'http://{}/validate'.format(VALIDATOR_ADDR)
        data = {
            'workflow_name': self.workflow_name,
            "batch": batch,
            "batch_id": batch["batch_id"],
            "first_run_finish_time": first_run_finish_time
        }
        log_message(f"[VALIDATE] batch_id:{batch['batch_id']}, transaction_list:{batch['transaction_list']}, first_run_finish_time: {first_run_finish_time}")
        return self._post_json(remote_url, data, f"validate batch {batch['batch_id']}") is not None
        

    def commit_batch(self, batch_id):
        remote_url = 'http://{}/commit'.format(VALIDATOR_ADDR)
        data = {
                'workflow_name': self.workflow_name,
                "batch_id": batch_id
            }
        return self._post_json(remote_url, data, f"commit batch {batch_id}") is not None

    def _post_json(self, url, data, context):
        started_at = time.time()
        log_message(f"[HTTP POST START] {context}: {url}")
        try:
            response = requests.post(url, json=data)
            response.raise_for_status()
            elapsed_s = time.time() - started_at
            log_message(
                f"[HTTP POST OK] {context}: {url}, status={response.status_code}, elapsed_s={elapsed_s:.3f}"
            )
            return response
        except requests.RequestException as exc:
            elapsed_s = time.time() - started_at
            log_message(f"[HTTP ERROR] {context}: {url}: {exc}, elapsed_s={elapsed_s:.3f}")
            return None

    def _join_and_report(self, jobs, context):
        if not jobs:
            return True
        gevent.joinall(jobs)
        ok = True
        for job in jobs:
            if job.exception is not None:
                ok = False
                log_message(f"[GEVENT ERROR] {context}: {job.exception}")
        return ok
