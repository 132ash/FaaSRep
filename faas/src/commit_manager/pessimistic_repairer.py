from gevent import monkey
monkey.patch_all()
import gevent
import gevent.lock
from repair_info import RepairInfo
import sys

sys.path.append('../../config')
import config

REPAIRED = 1
ABORTED = 2
WAITING = 3


class PessimisticRepairer:

    def __init__(self, workflow_name, repair_info: RepairInfo = None):
        self.workflow_name = workflow_name
        self.repair_info = repair_info
        self.tx_write_table_per_batch = {} 
        self.write_table_lock_per_batch = {}
        self.transaction_idx_per_batch = {}  
        self.last_subjection_for_tx_per_batch = {}  # {batch_id:{txid: last_tx_id}}
    
    def register_repair_info(self, batch_id, batch_write_set, batch_function_pos, transaction_list, last_tx):
        self.write_table_lock_per_batch[batch_id] = gevent.lock.BoundedSemaphore()
        self.transaction_idx_per_batch[batch_id] = {tx_id: idx for idx, tx_id in enumerate(transaction_list)}
        self.tx_write_table_per_batch[batch_id] = {}
        self.last_subjection_for_tx_per_batch[batch_id] = last_tx
        for tx_id in transaction_list:
            ws = batch_write_set.get(tx_id, {})
            for key, writer_func in ws.items():
                self.tx_write_table_per_batch[batch_id].setdefault(key, [None] * len(transaction_list))[self.transaction_idx_per_batch[batch_id][tx_id]] = {'tx_id': tx_id, 'func': writer_func, 'ip':batch_function_pos[tx_id][writer_func]['ip']}
                

    def prepare_pessimistic_info(self,batch_id, read_set,batch_function_pos,expired_keys, ready_tx_list):
        """
        based on current writer_list, prepare the expired keys and subjection info for the ready transactions.
        """
        self.write_table_lock_per_batch[batch_id].acquire()
        for tx_id in ready_tx_list:
            last_tx_idx = self.last_subjection_for_tx_per_batch[batch_id].get(tx_id, None)
            tx_dependency = {}
            # Find all (key, func) in the read set of tx_id
            if last_tx_idx:
                rs = read_set[tx_id]
                for func, func_rs in rs.items():
                    tx_dependency[func] = {}
                    for key in func_rs.keys():
                        # For each key, find the writer_list
                        writer_list = self.tx_write_table_per_batch[batch_id].get(key, [])
                        dependency = None
                        # Search for the first non-None writer before last_tx_idx
                        for prev_idx in range(last_tx_idx, -1, -1):
                            if writer_list[prev_idx] is not None:
                                dependency = writer_list[prev_idx]
                                break
                        # Store the dependency for this (key, func)
                        tx_dependency[func][key] = dependency
            self.repair_info.update_pessimistic_repair_metadata(batch_id, tx_id, tx_dependency, batch_function_pos[tx_id], expired_keys)
        self.write_table_lock_per_batch[batch_id].release()

    def modify_batch_write_table_for_abort(self, batch_id, batch_write_set, tx_id):
        tx_idx = self.transaction_idx_per_batch[batch_id][tx_id]
        ws = batch_write_set.get(tx_id, {})
        self.write_table_lock_per_batch[batch_id].acquire()
        for key, _ in ws.items():
            self.tx_write_table_per_batch[batch_id][key][tx_idx] = None
        self.write_table_lock_per_batch[batch_id].release()

    def pessimistic_get_commit_keys_per_ip(self, batch_id):
        batch_writeset = self.tx_write_table_per_batch[batch_id]
        commit_keys_per_ip = {}
        for key, writer_list in batch_writeset.items():
            # Find the rightmost non-None writer info
            for writer_info in writer_list[::-1]:
                if writer_info is not None:
                    ip = writer_info['ip']
                    tx_id = writer_info['tx_id']
                    func = writer_info['func']
                    commit_keys_per_ip.setdefault(ip, []).append(f"{tx_id}:PUT:{func}:{key}")
                    break
        return commit_keys_per_ip
            

    def clean_table_of_batch(self, batch_id):
        """
        Clean the write table and locks for the given batch_id.
        """
        self.tx_write_table_per_batch.pop(batch_id, None)
        self.write_table_lock_per_batch.pop(batch_id, None)
        # Remove transaction index mapping for the batch
        self.transaction_idx_per_batch.pop(batch_id, None)