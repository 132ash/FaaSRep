from gevent import monkey
monkey.patch_all()
import gevent
import gevent.lock
from repair_info import RepairInfo
import sys
from subprocess_log import log_message
from typing import Dict, List, Optional, Set

try:
    from models import WriterRef
except ImportError:  # pragma: no cover - package import path
    from .models import WriterRef

sys.path.append('../../config')
import config

REPAIRED = 1
ABORTED = 2
WAITING = 3


class PessimisticRepairer:

    def __init__(self,logger, workflow_name, repair_info: RepairInfo = None, function_pos=None):
        self.workflow_name = workflow_name
        self.logger = logger
        self.repair_info = repair_info
        self.function_pos = function_pos
        self.tx_write_table_per_batch: Dict[str, Dict[str, List[Optional[WriterRef]]]] = {}
        self.tx_read_table_per_batch = {}
        self.write_table_lock_per_batch = {}
        self.transaction_idx_per_batch = {}  
        self.tx_id_by_idx_per_batch = {}
        self.last_subjection_for_tx_per_batch = {}  # {batch_id:{txid: last_tx_id}}
        self.aborted_txs_per_batch: Dict[str, Set[str]] = {}
    
    def register_repair_info(self, batch_id, batch_read_set, batch_write_set, transaction_list, last_tx):
        log_message(self.logger, f"[PESSIMISTIC REGISTER] Registering repair info for batch {batch_id} with transactions: {transaction_list}")
        self.write_table_lock_per_batch[batch_id] = gevent.lock.BoundedSemaphore()
        self.transaction_idx_per_batch[batch_id] = {tx_id: idx for idx, tx_id in enumerate(transaction_list)}
        self.tx_id_by_idx_per_batch[batch_id] = list(transaction_list)
        self.tx_write_table_per_batch[batch_id] = {}
        self.last_subjection_for_tx_per_batch[batch_id] = last_tx
        self.tx_read_table_per_batch[batch_id] = batch_read_set
        self.aborted_txs_per_batch[batch_id] = set()
        for tx_id in transaction_list:
            ws = batch_write_set.get(tx_id, {})
            for key, writer_func in ws.items():
                self.tx_write_table_per_batch[batch_id].setdefault(
                    key, [None] * len(transaction_list)
                )[self.transaction_idx_per_batch[batch_id][tx_id]] = WriterRef(
                    batch_id, tx_id, writer_func
                )

    def prepare_pessimistic_info(self,batch_id,expired_keys, ready_tx_list):
        """
        based on current writer_list, prepare the expired keys and subjection info for the ready transactions.
        """
        dependencies_by_tx = {}
        lock = self.write_table_lock_per_batch[batch_id]
        lock.acquire()
        try:
            for tx_id in dict.fromkeys(ready_tx_list):
                tx_dependency = {}
                # Find all (key, func) in the read set of tx_id.
                rs = self.tx_read_table_per_batch.get(batch_id, {}).get(tx_id, {})
                for func, func_rs in rs.items():
                    tx_dependency[func] = {}
                    for key in func_rs.keys():
                        dependency = self._find_nearest_writer_before_last_tx(batch_id, tx_id, key)
                        tx_dependency[func][key] = (
                            [dependency.tx_id, dependency.func] if dependency is not None else None
                        )
                dependencies_by_tx[tx_id] = tx_dependency
        finally:
            lock.release()

        for tx_id, tx_dependency in dependencies_by_tx.items():
            log_message(self.logger, f"[PESSIMISTIC DEPENDENCY] tx {tx_id} dependency: {tx_dependency}")
            self.repair_info.update_pessimistic_repair_metadata(
                batch_id, tx_id, tx_dependency, expired_keys
            )

    def modify_batch_write_table_for_abort(self, batch_id, aborted_txs, batch_write_set, successed_tx_table):
        lock = self.write_table_lock_per_batch.get(batch_id)
        if lock is None:
            return
        lock.acquire()
        try:
            aborted_table = self.aborted_txs_per_batch.setdefault(batch_id, set())
            tx_idx_table = self.transaction_idx_per_batch.get(batch_id, {})
            for aborted_tx_id in dict.fromkeys(aborted_txs):
                successed_tx_table.pop(aborted_tx_id, None)
                tx_idx = tx_idx_table.get(aborted_tx_id)
                if tx_idx is None:
                    continue
                aborted_table.add(aborted_tx_id)
                ws = batch_write_set.get(aborted_tx_id, {})
                for key in ws:
                    writer_list = self.tx_write_table_per_batch.get(batch_id, {}).get(key)
                    if writer_list is not None and tx_idx < len(writer_list):
                        writer_list[tx_idx] = None
        finally:
            lock.release()
        log_message(self.logger, f"[PESSIMISTIC ABORT] Modified write table for aborted transactions {aborted_txs} in batch {batch_id}. Remaining transactions: {successed_tx_table.keys()}, write set: {self.tx_write_table_per_batch[batch_id]}")

    def pessimistic_get_commit_keys(self, batch_id):
        batch_writeset = self.tx_write_table_per_batch[batch_id]
        commit_keys_all = set()
        for key, writer_list in batch_writeset.items():
            # Find the rightmost non-None writer info
            for writer_info in writer_list[::-1]:
                if writer_info is not None:
                    commit_keys_all.add(key)
                    break
        log_message(self.logger, f"[PESSIMISTIC COMMIT KEYS] Batch {batch_id} all commit keys: {commit_keys_all}")
        return commit_keys_all
            

    def clean_table_of_batch(self, batch_id):
        """
        Clean the write table and locks for the given batch_id.
        """
        self.tx_write_table_per_batch.pop(batch_id, None)
        self.write_table_lock_per_batch.pop(batch_id, None)
        self.tx_read_table_per_batch.pop(batch_id, None)
        
        # Remove transaction index mapping for the batch
        self.transaction_idx_per_batch.pop(batch_id, None)
        self.tx_id_by_idx_per_batch.pop(batch_id, None)
        self.last_subjection_for_tx_per_batch.pop(batch_id, None)
        self.aborted_txs_per_batch.pop(batch_id, None)

    def _find_nearest_writer_before_last_tx(self, batch_id, tx_id, key):
        writer_list = self.tx_write_table_per_batch.get(batch_id, {}).get(key)
        if not writer_list:
            return None
        last_tx_id = self.last_subjection_for_tx_per_batch.get(batch_id, {}).get(tx_id)
        if not last_tx_id:
            return None
        last_tx_idx = self.transaction_idx_per_batch.get(batch_id, {}).get(last_tx_id)
        if last_tx_idx is None:
            return None
        for prev_idx in range(last_tx_idx, -1, -1):
            writer = writer_list[prev_idx]
            if writer is not None:
                return writer
        return None
