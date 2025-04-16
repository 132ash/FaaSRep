from gevent import event

class RunningTXTable:
    def __init__(self):
        self.running_txs = {}
    
    def registerTX(self, tx_id, tx_params):
        self.running_txs[tx_id] = {"params":tx_params,"finished":False,"abort":False ,"cond":event.Event()}

    def finishTX(self, tx_id):
        validate_latency = self.running_txs[tx_id]["validate_latency"]
        validate_time_inside_validator = self.running_txs[tx_id]["validate_time_inside_validator"]
        self.running_txs.pop(tx_id)
        return validate_latency, validate_time_inside_validator

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
        self.running_txs[tx_id]['finished'] = False
        self.running_txs[tx_id]['abort'] = False
        condition = self.running_txs[tx_id]['cond']
        condition.clear()
    
    def TxFinished(self, tx_id):
        return self.running_txs[tx_id]['finished']

    def notifyTX(self, transaction_id_list, validate_latency, validate_time_inside_validator, abort = False):
        if abort:
            self.running_txs[transaction_id_list[0]][abort] = True
            self.running_txs[transaction_id_list[0]]['finished'] = True
            self.running_txs[transaction_id_list[0]]['cond'].set()
        else:
            for tx_id in transaction_id_list:
                condition = self.running_txs[tx_id]['cond']
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]["validate_latency"] = validate_latency
                self.running_txs[tx_id]['validate_time_inside_validator']=validate_time_inside_validator
                condition.set()
                print(f"notified {tx_id}")