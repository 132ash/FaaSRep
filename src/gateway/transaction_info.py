from gevent import event
from gateway_repo import Repository
import time
import logging

class RunningTXTable:
    def __init__(self, repo:Repository):
        self.running_txs = {}
        self.repo:Repository = repo
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,"abort":False ,'Abort_type':'', "cond":event.Event()}

    def finishTX(self, tx_id):
        validate_latency = self.running_txs[tx_id]["validate_latency"]
        first_run_finish_time = self.running_txs[tx_id]["first_run_finish_time"]
        validate_time_inside_validator = self.running_txs[tx_id]["validate_time_inside_validator"]
        self.running_txs.pop(tx_id)
        return first_run_finish_time, validate_latency, validate_time_inside_validator

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
        while not self.running_txs[tx_id]['finished']:
            condition.wait()
        if self.running_txs[tx_id]['abort']:
            return True, self.running_txs[tx_id]['Abort_type']
        return False, ''
    
    def resetTX(self, tx_id):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, transaction_id, first_run_finish_time, abort = False, Abort_type=''):
        if abort:
            # self.repo.delete_latency(transaction_id)
           # logging.info(f"transaction {transaction_id} aborted.")
            self.running_txs[transaction_id]['abort'] = True
            self.running_txs[transaction_id]['Abort_type'] = Abort_type
            self.running_txs[transaction_id]['finished'] = True
            self.running_txs[transaction_id]['cond'].set()
        else:
            condition = self.running_txs[transaction_id]['cond']
            self.running_txs[transaction_id]['finished'] = True
            self.running_txs[transaction_id]["first_run_finish_time"] = first_run_finish_time
            self.running_txs[transaction_id]["validate_latency"] = 0
            self.running_txs[transaction_id]['validate_time_inside_validator'] = 0
            condition.set()