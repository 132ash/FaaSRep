from gevent import monkey
monkey.patch_all()
import sys
import requests
import gevent
from gevent.queue import Queue
import time 
import datetime
from repair_info import RepairInfo
from validator import BatchValidator
from TX_timestamp import TimeStampAllocator, BatchVersion
from repair_engine import RepairEngine
import json
import sys

def get_timestamp():
    # 获取当前时间，并格式化为字符串，精确到微秒
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    return timestamp


sys.path.append('../../config')
import config

from flask import Flask, request
app = Flask(__name__)


Timestamp_allocator = TimeStampAllocator()
repair_info = RepairInfo(config.FAST_PATH)
Validator = BatchValidator(Timestamp_allocator, repair_info)
repairer = RepairEngine(repair_info)
GATEWAY_ADDR = config.GATEWAY_ADDR
print(f"Validator and repairer started. initial key version: {Validator.global_table}")

validate_interval = 0.005

class ValidateDispatcher:
    def __init__(self):
        self.rq = Queue()
        self._validate_loop()
    
    def notify_gateway(self, transaction_id_list, success:bool, start_time, validate_time_inside_validator):
        url = 'http://{}/notify'.format(GATEWAY_ADDR)
        data = {
            'transaction_id_list': transaction_id_list,
            'success': success,
            "start_time": start_time,
            'validate_time_inside_validator':validate_time_inside_validator
        }
        r = requests.post(url, json=data)
        return r.json()
    
    def _validate_loop(self):
        gevent.spawn_later(validate_interval, self._validate_loop)
        gevent.spawn(self.handle_validate)

    def handle_validate(self):
        start_time = time.time()
        if self.rq.empty():
            return
        data = self.rq.get()
        batch = data['batch']
        batch_id = batch['batch_id']
        logging.info(f"Start processing batch: {batch_id}")
        workflow_name_per_tx = batch['workflow_name']
        function_pos_per_tx = batch['function_pos']
        worker_ip_set = batch['worker_set'].keys()
        transaction_list = batch['transaction_list']
        read_set = batch['read_set']
        write_set = batch['write_set']
        RYW_subjection = batch['RYW_subjection']

        logging.info(f"received batch: {batch_id}, workflow_name_per_tx: {workflow_name_per_tx}, function_pos_per_tx: {function_pos_per_tx}, transaction_list: {transaction_list}, read_set: {read_set}, write_set: {write_set}, RYW_subjection: {RYW_subjection}")

        commitTime = Timestamp_allocator.allocate_timestamp(batch_id)
        version = BatchVersion(commitTime)

        repair_info.batch_init(batch_id)
        expired_keys, confilcted = Validator.validate(batch_id, workflow_name_per_tx, read_set, write_set, transaction_list, function_pos_per_tx, RYW_subjection)

        repair_successful = True
        validate_time_inside_validator = time.time() - start_time
        if confilcted:
            logging.info(f"trigger repair for batch: {batch_id}. expired_keys: {expired_keys}")
            repair_successful = repairer.trigger_repair(batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, worker_ip_set ,config.FAST_PATH)

        if repair_successful:
            TXid_list = Validator.commit_batch(batch_id, version.to_string(), function_pos_per_tx)
            self.notify_gateway(TXid_list, True, start_time, validate_time_inside_validator)
        else:
            logging.error(f"Validation failed for batch: {batch_id}")

dispatcher = ValidateDispatcher()
# receive a set of rw sets and validate them: lock, get delta set and send to gateway.
# rerun or directly commit.
# used in first or second phase.
# read set: {func: {key: version}}  write set: {key: {ip:func_ip, func:func}}
@app.route('/validate', methods=['POST'])
def validate_tx():
    data = request.get_json(force=True, silent=True)
    dispatcher.rq.put(data)
    return json.dumps({'status': 'processing'})  
    
@app.route('/fin_repair', methods = ['POST'])
def repair_finish():
    data = request.get_json(force=True, silent=True)
    batch_id = data['batch_id']
    repairer.notify_batch(batch_id, True)
    return json.dumps({'status': 'successed'})



# python3 proxy.py 192.168.162.132 9000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()