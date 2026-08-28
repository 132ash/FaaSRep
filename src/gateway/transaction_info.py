from gevent import event
import sys
import requests
import time
import logging
sys.path.append('../../config')
import config

VALIDATOR_ADDR = config.VALIDATOR_ADDR


class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
        self.last_transition_timestamp = time.time()
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.last_transition_timestamp = time.time()
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,"abort":False ,"cond":event.Event(), 'pessimistic':False, 'error': ''}

    def finishTX(self, tx_id):
        first_run_finish_time = self.running_txs[tx_id]["first_run_finish_time"]
        repair_start_time = self.running_txs[tx_id]["repair_start_time"]
        repair_finish_time = self.running_txs[tx_id]["repair_finish_time"]
        commit_finish_time = self.running_txs[tx_id].get("commit_finish_time", repair_finish_time)
        notify_received_time = self.running_txs[tx_id].get("notify_received_time", commit_finish_time)
        pessimistic = self.running_txs[tx_id]['pessimistic']
        self.running_txs.pop(tx_id)
        return first_run_finish_time, repair_start_time, repair_finish_time, commit_finish_time, notify_received_time, pessimistic

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
        while not self.running_txs[tx_id]['finished']:
            condition.wait()
        if self.running_txs[tx_id]['abort']:
            logging.info("transaction %s aborted", tx_id)
            return True
        return False
    
    def resetTX(self, tx_id):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, transaction_id_list, first_run_finish_time, repair_start_time, repair_finish_time, commit_finish_time=None, notify_received_time=None, abort = False, pessimistic_txs=None, abort_errors=None):
        self.last_transition_timestamp = time.time()
        if pessimistic_txs is None:
            pessimistic_txs = {}
        if abort_errors is None:
            abort_errors = {}
        if abort:
            for tx_id in transaction_id_list:
                self.running_txs[tx_id]['abort'] = True
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]['error'] = abort_errors.get(tx_id, '')
                self.running_txs[tx_id]['cond'].set()
        else:
            for tx_id in transaction_id_list:
                self.running_txs[tx_id]['pessimistic'] = pessimistic_txs.pop(tx_id, False)
                condition = self.running_txs[tx_id]['cond']
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]["first_run_finish_time"] = first_run_finish_time
                self.running_txs[tx_id]["repair_start_time"] = repair_start_time
                self.running_txs[tx_id]['repair_finish_time']=repair_finish_time
                self.running_txs[tx_id]['commit_finish_time'] = commit_finish_time if commit_finish_time else repair_finish_time
                self.running_txs[tx_id]['notify_received_time'] = notify_received_time if notify_received_time else time.time()
                condition.set()
