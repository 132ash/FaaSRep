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

    def notifyTX(self, transaction_id_list, first_run_finish_time, validate_latency, validate_time_inside_validator, abort = False):
        if abort:
            print(f"CONCORD: transaction {transaction_id_list[0]} aborted.")
            self.concord_abort(transaction_id_list[0])
        else:
            for tx_id in transaction_id_list:
                condition = self.running_txs[tx_id]['cond']
                self.running_txs[tx_id]['finished'] = True
                self.running_txs[tx_id]["first_run_finish_time"] = first_run_finish_time
                self.running_txs[tx_id]["validate_latency"] = validate_latency
                self.running_txs[tx_id]['validate_time_inside_validator']=validate_time_inside_validator
                condition.set()
                print(f"notified {tx_id}")

    def concord_abort(self, transaction_id):
        tx_table = self.running_txs.get(transaction_id, None)
        if not tx_table or tx_table['concord']:
            return
        tx_table['concord'] = True
        workflow_name = tx_table['workflow']
        url = f"http://{VALIDATOR_ADDR}/concord_abort"
        data = {'workflow_name': workflow_name, 'transaction_id': transaction_id}
        requests.post(url, json=data)
        self.running_txs[transaction_id]['abort'] = True
        self.running_txs[transaction_id]['finished'] = True
        self.running_txs[transaction_id]['cond'].set()
