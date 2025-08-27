from gevent import event
import sys
import time
sys.path.append('../../config')
import config

VALIDATOR_ADDR = config.VALIDATOR_ADDR


class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,
                                   "abort":False,'finish_time':None,'validate_latency':None,
                                   'commit_latency':None,'active_abort':False,"cond":event.Event()}

    def finishTX(self, tx_id):
        commit_latency = self.running_txs[tx_id]["commit_latency"]
        self.running_txs.pop(tx_id)
        return commit_latency

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
        while not self.running_txs[tx_id]['finished']:
            condition.wait()
        if self.running_txs[tx_id]['abort']:
            return True, self.running_txs[tx_id]['active_abort'], self.running_txs[tx_id]['finish_time'], self.running_txs[tx_id]['validate_latency']
        return False, False, self.running_txs[tx_id]['finish_time'], self.running_txs[tx_id]['validate_latency']

    def resetTX(self, tx_id):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, commited_txs, aborted_txs, validate_latency, commit_latency, self_abort = False):
        for tx_id in aborted_txs:
            self.running_txs[tx_id]['abort'] = True
            self.running_txs[tx_id]["finish_time"] = time.time()
            self.running_txs[tx_id]["validate_latency"] = validate_latency
            self.running_txs[tx_id]['commit_latency'] = commit_latency
            self.running_txs[tx_id]['active_abort'] = self_abort
            self.running_txs[tx_id]['finished'] = True
            self.running_txs[tx_id]['cond'].set()
        for tx_id in commited_txs:
            self.running_txs[tx_id]["finish_time"] = time.time()
            self.running_txs[tx_id]["validate_latency"] = validate_latency
            self.running_txs[tx_id]['commit_latency'] = commit_latency
            self.running_txs[tx_id]['finished'] = True
            self.running_txs[tx_id]['cond'].set()
