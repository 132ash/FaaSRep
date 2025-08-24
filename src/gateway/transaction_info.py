from gevent import event
import sys
import time
sys.path.append('../../config')
import config



class RunningTXTable:
    def __init__(self):
        self.running_txs = {}

    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,'finish_time':None, "abort":False, "Abort_type":'',"cond":event.Event(), 'commit_latency':0, 'term':0}

    def finishTX(self, tx_id):
        state = self.running_txs.pop(tx_id)
        return state['term'], state['commit_latency']

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
        while not self.running_txs[tx_id]['finished']:
            condition.wait()
        if self.running_txs[tx_id]['abort']:
            return True, self.running_txs[tx_id]["Abort_type"], self.running_txs[tx_id]['finish_time']
        return False, '', self.running_txs[tx_id]['finish_time']
    
    def resetTX(self, tx_id, term):
        self.running_txs[tx_id]['abort'] = False
        self.running_txs[tx_id]['finished'] = False
        self.running_txs[tx_id]['term'] = term
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, tx_id, term, commit_latency, abort = False, Abort_type=''):
        if abort:
            if term != self.running_txs[tx_id]['term']:
                return
            self.running_txs[tx_id]['abort'] = True
            self.running_txs[tx_id]["Abort_type"] = Abort_type
            self.running_txs[tx_id]['finished'] = True
            self.running_txs[tx_id]['finish_time'] = time.time()
            self.running_txs[tx_id]['cond'].set()
            # logging.info(f"[ABORT] tx_id {tx_id} Aborted. Need to retry.")
        else:
            condition = self.running_txs[tx_id]['cond']
            self.running_txs[tx_id]['finished'] = True
            self.running_txs[tx_id]['finish_time'] = time.time()
            self.running_txs[tx_id]['commit_latency'] = commit_latency
            condition.set()
            # logging.info(f"[FINISH] tx_id {tx_id} finished running.")

