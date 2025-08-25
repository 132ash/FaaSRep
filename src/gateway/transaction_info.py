from gevent import event
import sys
import requests
sys.path.append('../../config')
import config

VALIDATOR_ADDR = config.VALIDATOR_ADDR


class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,"abort":False ,"cond":event.Event(), 'pessimistic':False}

    def finishTX(self, tx_id):
        first_run_finish_time = self.running_txs[tx_id]["first_run_finish_time"]
        repair_start_time = self.running_txs[tx_id]["repair_start_time"]
        repair_finish_time = self.running_txs[tx_id]["repair_finish_time"]
        pessimistic = self.running_txs[tx_id]['pessimistic']
        self.running_txs.pop(tx_id)
        return first_run_finish_time, repair_start_time, repair_finish_time, pessimistic

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
        while not self.running_txs[tx_id]['finished']:
            condition.wait()
        if self.running_txs[tx_id]['abort']:
            print(f"transaction {tx_id} aborted")
            return True
        return False
    
    def resetTX(self, tx_id):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, transaction_id_list, first_run_finish_time, repair_start_time, repair_finish_time, abort = False, pessimistic_txs={}):
        if abort:
            for tx_id in transaction_id_list:
                self.running_txs[tx_id]['abort'] = True
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]['cond'].set()
        else:
            for tx_id in transaction_id_list:
                self.running_txs[tx_id]['pessimistic'] = pessimistic_txs.pop(tx_id, False)
                condition = self.running_txs[tx_id]['cond']
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]["first_run_finish_time"] = first_run_finish_time
                self.running_txs[tx_id]["repair_start_time"] = repair_start_time
                self.running_txs[tx_id]['repair_finish_time']=repair_finish_time
                condition.set()
