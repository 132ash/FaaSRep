import threading

class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, tx_id, tx_params):
        self.running_txs[tx_id] = {"params":tx_params,"finished":False, "cond":threading.Condition()}

    def finishTX(self, tx_id):
        self.running_txs.pop(tx_id)

    def waitTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        with condition:
            if not self.running_txs[tx_id]['finished']:
                condition.wait()

    def notifyTX(self, tx_id):
        condition = self.running_txs[tx_id]['cond']
        self.running_txs[tx_id]['finished'] = True
        with condition:
            condition.notify_all()
        print(f"notified {tx_id}")