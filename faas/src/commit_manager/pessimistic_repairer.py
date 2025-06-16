from gevent import monkey
monkey.patch_all()
import gevent
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
        self.subjection_table_per_batch = {} 
        self.RYW_info_per_batch = {}
        self.transaction_list_per_batch = {}  # {batch_id: [tx_id1, tx_id2, ...]}
        self.transaction_idx_per_batch = {}  # {batch_id: {tx_id: idx}}
        self.function_pos_per_batch = {}  # {batch_id: {tx_id: {func_name: {'ip': ip, 'pos': pos}}}}
        self.batch_state = {}  # {batch_id: state}, where state is one of REPAIRED, ABORTED, WAITING
    
    def register_repair_info(self, batch_id, batch):
        self.transaction_list_per_batch[batch_id] = batch['transaction_list']
        num_tranasction = len(batch['transaction_list'])
        self.function_pos_per_batch[batch_id] = batch['function_pos']
        self.transaction_idx_per_batch[batch_id] = {tx_id: idx for idx, tx_id in enumerate(batch['transaction_list'])}
        self.subjection_table_per_batch[batch_id] = {}
        ready_txs = {}
        for tx_id in batch['transaction_list']:
            self.batch_state[tx_id] = {'batch':True, 'tx':True}
            ready_txs[tx_id] = True
            ws = batch['write_set'].get(tx_id, {})
            for key, writer_func in ws.items():
                self.subjection_table_per_batch[batch_id].setdefault(key, [None] * num_tranasction)[self.transaction_idx_per_batch[batch_id][tx_id]] = (tx_id, writer_func)
        


    def prepare_pessimistic_info(self, batch_id, fin_tx, fin_batch):
        """
        Trigger the cascaded repair process.
        :param batch_id: The ID of the batch.
        :param fin_tx: The finished transaction.
        :param fin_batch: The finished batch.
        :return: A tuple of ready transactions and expired keys.
        """
        self.repair_info.cascaded_trigger(batch_id, fin_tx, fin_batch)
        return self.repair_info.get_ready_transactions(batch_id), self.repair_info.get_expired_keys(batch_id)
        

