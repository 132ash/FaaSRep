from gevent import event
import sys
import time
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import config

VALIDATOR_ADDR = config.VALIDATOR_ADDR


class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, workflow, tx_id, tx_params):
        self.running_txs[tx_id] = {'workflow':workflow, "params":tx_params,"finished":False,
                                   "abort":False,'finish_time':None,'validate_latency':None,
                                   'commit_latency':None,'active_abort':False,"cond":event.Event(),
                                   'term': 0, 'birth_seq': None, 'abort_type': None, 'metrics': {}, 'error': None}

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

    def set_boki_attempt(self, tx_id, term, birth_seq):
        tx = self.running_txs[tx_id]
        tx['term'] = term
        tx['birth_seq'] = birth_seq
        tx['abort'] = False
        tx['finished'] = False
        tx['abort_type'] = None
        tx['metrics'] = {}
        tx['error'] = None
        tx['cond'].clear()

    def waitBoki(self, tx_id, timeout=None):
        tx = self.running_txs[tx_id]
        if not tx['finished']:
            tx['cond'].wait(timeout=timeout)
        if not tx['finished']:
            return {'status': 'error', 'term': tx['term'], 'error': 'workflow notification timeout'}
        return {'status': 'aborted' if tx['abort'] else ('error' if tx['error'] else 'committed'),
                'term': tx['term'], 'abort_type': tx['abort_type'], 'metrics': tx['metrics'], 'error': tx['error'],
                'finish_time': tx['finish_time']}
    
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

    def notifyBoki(self, tx_id, term, status, abort_type=None, metrics=None, error=None):
        tx = self.running_txs.get(tx_id)
        if tx is None or int(term) != tx['term'] or tx['finished']:
            return False
        tx['finish_time'] = time.time()
        tx['metrics'] = metrics or {}
        tx['abort_type'] = abort_type
        tx['error'] = error if status == 'error' else None
        tx['abort'] = status == 'aborted'
        tx['finished'] = True
        tx['cond'].set()
        return True
