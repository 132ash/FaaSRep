import threading

class RunningTXTable:
    def __init__(self):
        self.running_txs = {str: str}
    
    def registerTX(self, tx_id, tx_params):
        self.running_txs[tx_id] = {"params":tx_params,"finished":threading.Condition()}

    def finishTX(self, tx_id):
        self.running_txs.pop(tx_id)