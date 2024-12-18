import sys
from gevent import monkey
monkey.patch_all()
import requests
import datetime

from validator import TxValidator, TxVersion
from TX_timestamp import TimeStampAllocator, TxVersion
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
Validator = TxValidator(Timestamp_allocator)
repairer = RepairEngine()
GATEWAY_ADDR = config.GATEWAY_ADDR
print(f"Validator and repairer started. initial key version: {Validator.global_table}")

def notify_gateway(transaction_id, success:bool):
    url = 'http://{}/notify'.format(GATEWAY_ADDR)
    data = {
        'transaction_id': transaction_id,
        'success': success
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
    read_set = data['read_set']
    write_set = data['write_set']
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    function_pos = data.get('function_pos', {})
    commitTime = Timestamp_allocator.allocate_timestamp(transaction_id)
    version = TxVersion(transaction_id, commitTime)
    # get expired keys.
    expired_keys, confilcted = Validator.validate(transaction_id, read_set, write_set, function_pos)
    for k, v in expired_keys.items():
        expired_keys[k] = list(v)
    print(f"expired_keys: {expired_keys}")
    repair_successful = repairer.trigger_repair(transaction_id, workflow_name, expired_keys, confilcted, function_pos)
    if repair_successful:
        Validator.commit_tx(transaction_id, workflow_name, version.to_string())
        notify_gateway(transaction_id, True)
        return json.dumps({'status': 'successed'})
    else:
        return json.dumps({'status': 'failed'})
    
@app.route('/fin_repair', methods = ['POST'])
def repair_finish():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    repairer.notify_Tx(transaction_id)
    return json.dumps({'status': 'successed'})


from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()