from gevent import monkey
monkey.patch_all()
import sys
import requests
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
repair_info = RepairInfo()
Validator = BatchValidator(Timestamp_allocator, repair_info)
repairer = RepairEngine(repair_info)
GATEWAY_ADDR = config.GATEWAY_ADDR
print(f"Validator and repairer started. initial key version: {Validator.global_table}")

def notify_gateway(transaction_id_list, success:bool, start_time):
    url = 'http://{}/notify'.format(GATEWAY_ADDR)
    data = {
        'transaction_id_list': transaction_id_list,
        'success': success,
        "start_time": start_time
    }
    r = requests.post(url, json=data)
    return r.json()

# receive a set of rw sets and validate them: lock, get delta set and send to gateway.
# rerun or directly commit.
# used in first or second phase.
# read set: {func: {key: version}}  write set: {key: {ip:func_ip, func:func}}
@app.route('/validate', methods = ['POST'])
def validate_tx():
    data = request.get_json(force=True, silent=True)
    start_time = time.time()
    batch = data['batch']
    # using the first txid as the batch id.
    batch_id = batch['batch_id']
    workflow_name_per_tx = batch['workflow_name'] # {txid: workflow_name}
    function_pos_per_tx = batch['function_pos'] # {txid: {func: IP}}
    worker_ip_set = batch['worker_set'].keys() # [IP1, IP2,...]
    transaction_list = batch['transaction_list'] # [txid1, txid2,...]
    read_set = batch['read_set'] #[{txid:xx, read_set:{}},...]
    write_set = batch['write_set'] #[{txid:xx, write_set:{}},...]
    RYW_subjection = batch['RYW_subjection']
    logging.info(f"received batch: {batch_id}, workflow_name_per_tx: {workflow_name_per_tx}, function_pos_per_tx: {function_pos_per_tx}, transaction_list: {transaction_list}, read_set: {read_set}, write_set: {write_set}, RYW_subjection: {RYW_subjection}")
    commitTime = Timestamp_allocator.allocate_timestamp(batch_id)
    print(f"acquired timestamp: {time.time() - start_time}")
    version = BatchVersion(commitTime)
    # start validating.
    repair_info.batch_init(batch_id)
    expired_keys, confilcted = Validator.validate(batch_id, workflow_name_per_tx, read_set, write_set, transaction_list, function_pos_per_tx, RYW_subjection)
 
    # start repairing.
    print(f"expired_keys: {expired_keys}, time:{time.time() - start_time}")
    repair_successful = True
    if confilcted:
        logging.info(f"trigger repair for batch: {batch_id}. expired_keys: {expired_keys}")
        repair_successful = repairer.trigger_repair(batch_id, transaction_list, workflow_name_per_tx, function_pos_per_tx, expired_keys, worker_ip_set)
    print(f"repair_successful: {repair_successful}, time:{time.time() - start_time}")

    if repair_successful:
        TXid_list = Validator.commit_batch(batch_id, version.to_string(), function_pos_per_tx)
        notify_gateway(TXid_list, True,  start_time)
        return json.dumps({'status': 'successed'})
    else:
        return json.dumps({'status': 'failed'})
    
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