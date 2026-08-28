from gevent import monkey

monkey.patch_all()
import logging
import sys
sys.path.append('../../config')
import config

REPAIRED = config.REPAIRED
ABORTED = config.ABORTED    
WAITING = config.RUNNING


OPT_REPAIR = config.OPT_REPAIR
PESSI_REPAIR = config.PESSI_REPAIR

PESSIMISTIC_REPAIR = not config.OPTIMISTIC_REPAIR
VALIDATOR_ADDR = config.VALIDATOR_ADDR

class PessimisticBatchState:
    def __init__(self, batch_id, tx_list, batch_size):
        self.batch_size = batch_size
        self.batch_id = batch_id
        self.transaction_list = tx_list
        self.next_txs_after_batch = {} # {successor_batchid: [txid1, txid2, ...]}
        self.fin_consecutive_cnt = -1
        self.finished_tx_list = [None] * len(tx_list) 
        self.tx_idx = {txid: idx for idx, txid in enumerate(tx_list)}
        self.pessi_transaction_info = {txid:{'next_txs':[], 'prev_fin_cnt':0, 'fin_repair':False} for txid in tx_list}
        self.pessimistic_repair_ready = {txid:False for txid in tx_list}

    def retain_transactions(self, retained_transaction_list):
        """Shrink a pre-registered batch before any transaction is repaired."""
        retained = set(retained_transaction_list)
        self.transaction_list = [
            tx_id for tx_id in self.transaction_list if tx_id in retained
        ]
        self.batch_size = len(self.transaction_list)
        self.finished_tx_list = [None] * self.batch_size
        self.tx_idx = {
            tx_id: index for index, tx_id in enumerate(self.transaction_list)
        }
        self.pessi_transaction_info = {
            tx_id: self.pessi_transaction_info[tx_id]
            for tx_id in self.transaction_list
        }
        self.pessimistic_repair_ready = {
            tx_id: self.pessimistic_repair_ready[tx_id]
            for tx_id in self.transaction_list
        }
        self.fin_consecutive_cnt = -1

    def trigger_successor(self, next_trigger_txs, ready_txs):
        for tx_id in next_trigger_txs:
            self.pessi_transaction_info[tx_id]['prev_fin_cnt'] -= 1
            if self.pessi_transaction_info[tx_id]['prev_fin_cnt'] == 0:
                self.pessimistic_repair_ready[tx_id] = True
                ready_txs.setdefault(self.batch_id, []).append(tx_id)
       
    def init_tx_info(self, ready_txs, tx_sub, batch_successors):
        for tx_id in batch_successors:
            ready_txs.pop(tx_id, None)
            self.pessi_transaction_info[tx_id]['prev_fin_cnt'] += 1
        for prev_tx_id, next_txs in tx_sub.items():
            self.pessi_transaction_info[prev_tx_id]['next_txs'] = next_txs
            for tx_id in next_txs:
                ready_txs.pop(tx_id, None)
                self.pessi_transaction_info[tx_id]['prev_fin_cnt'] += 1
        for tx_id in ready_txs:
            self.pessimistic_repair_ready[tx_id] = True

    def modify_batch_successors(self, next_batch_id, next_batch_txs, batch_successors):
        batch_successors.extend(next_batch_txs)
        self.next_txs_after_batch.setdefault(next_batch_id, []).extend(next_batch_txs)
        # logging.info(f"[PESSIMISTIC REPAIR] Batch {self.batch_id} modified successors: {next_batch_id} with transactions {next_batch_txs}")

    def transaction_finish(self, tx_id, ready_txs):
        txs_to_be_triggered_by_prev_finish = []
        finish_idx = self.tx_idx[tx_id]
        self.finished_tx_list[self.tx_idx[tx_id]] = True
        if self.fin_consecutive_cnt == finish_idx - 1:
            while self.fin_consecutive_cnt < self.batch_size - 1 and self.finished_tx_list[self.fin_consecutive_cnt + 1] is not None:
                self.fin_consecutive_cnt += 1
                current_fin_tx_id = self.transaction_list[self.fin_consecutive_cnt]
                txs_to_be_triggered_by_prev_finish.extend(self.pessi_transaction_info[current_fin_tx_id]['next_txs'])
        self.trigger_successor(txs_to_be_triggered_by_prev_finish, ready_txs)


class OptimisticTransactionState:
    def __init__(self, batch_id, tx_id):
        self.need_pessimistic_repair = False
        self.optimistic_repair_state = WAITING
        self.batch_id = batch_id
        self.transaction_id = tx_id
        self.transaction_subjection = []

    def modify_transaction_subjection(self, tx_sub_inside_batch:list):
        """
        Update the subjection info for the given transaction.
        sub_per_tx: {prev_tx_id: {next_tx:True}}
        """
        if self.optimistic_repair_state == ABORTED:
            return ABORTED
        self.transaction_subjection.extend(tx_sub_inside_batch)
        # logging.info(f"[OPTIMISTIC SUBJECTION] Transaction {self.transaction_id} in batch {self.batch_id} updated subjection: {self.transaction_subjection}")
        return self.optimistic_repair_state
    
    def optimistic_state_change_after_repair(self, optimistic_repair_mode, repair_state):
        """
        Update the optimistic repair state after repair. return the repair is rejected or not.
        """
        # An application-requested abort is terminal.  It may race with the
        # validator marking this transaction for pessimistic repair; accepting
        # the promotion first would otherwise discard the abort after the
        # container has already cleaned up its transaction context.
        if repair_state == ABORTED:
            self.optimistic_repair_state = ABORTED
            self.need_pessimistic_repair = False
            return False, self.transaction_subjection
        if self.need_pessimistic_repair and optimistic_repair_mode == OPT_REPAIR:
            return True, []
        self.optimistic_repair_state = repair_state
        return False, []
