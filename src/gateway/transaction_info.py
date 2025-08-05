from gevent import event
import sys
import logging
sys.path.append('../../config')
import config



class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,"abort":False ,"cond":event.Event(), 'concord':False}

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
            return True
        return False
    
    def resetTX(self, tx_id):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, transaction_id_list, first_run_finish_time, validate_latency, validate_time_inside_validator, abort = False):
        if abort:
            tx_id = transaction_id_list[0]
            self.running_txs[tx_id]['abort'] = True
            self.running_txs[tx_id]['finished'] = True
            self.running_txs[tx_id]['cond'].set()
            # logging.info(f"[ABORT] tx_id {tx_id} Aborted. Need to retry.")
        else:
            for tx_id in transaction_id_list:
                condition = self.running_txs[tx_id]['cond']
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]["first_run_finish_time"] = first_run_finish_time
                self.running_txs[tx_id]["validate_latency"] = validate_latency
                self.running_txs[tx_id]['validate_time_inside_validator']=validate_time_inside_validator
                condition.set()
                # logging.info(f"[FINISH] tx_id {tx_id} finished running.")

