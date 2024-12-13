import sys
import requests
from gevent import monkey
monkey.patch_all()
import datetime

from validator import TxValidator, TxVersion
from components.faas.src.commit_manager.TX_timestamp import TimeStampAllocator 
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


Validator = TxValidator()
Timestamp_allocator = TimeStampAllocator()
GATEWAY_ADDR = config.GATEWAY_ADDR

# TODO: trigger repair via send request to every workersp
def trigger_repair(transaction_id, expired_keys):
    url = 'http://{}/repair'.format(GATEWAY_ADDR)
    print(f"sending repair req to {url}, with expired_keys: {expired_keys}")
    data = {
        'transaction_id': transaction_id,
        'expired_keys': expired_keys
    }
    r = requests.post(url, json=data)
    return r.json()

# TODO： trigger commit via send request to every workersp
def trigger_commit(transaction_id):
    pass

def notify_gateway(transaction_id):
    url = 'http://{}/notify'.format(GATEWAY_ADDR)
    print(f"sending return req to {url}")
    data = {
        'transaction_id': transaction_id
    }
    r = requests.post(url, json=data)
    return r.json()

# receive a set of rw sets and validate them: lock, get delta set and send to gateway.
# rerun or directly commit.
# used in first or second phase.
@app.route('/validate', methods = ['POST'])
def validate_tx():
    data = request.get_json(force=True, silent=True)
    commitTime = get_timestamp()
    read_set = data['read_set']
    write_set = data['write_set']
    transaction_id = data['transaction_id']
    version = TxVersion(transaction_id, commitTime)
    expired_keys, confilcted = Validator.validate(transaction_id, read_set, write_set)
    repair_res = trigger_repair(transaction_id, expired_keys)
    if repair_res['status'] == 'ok':
        r = trigger_commit(transaction_id)
        if r['status'] == 'ok':
            notify_gateway(transaction_id)
            Validator.update_global_table(write_set, version)
            return json.dumps({'status': 'ok'})
        else:
            return json.dumps({'status': 'failed'})
    else:
        return json.dumps({'status': 'failed'})


# final commit, release lock and notify the gateway. 
@app.route('/commit', methods = ['POST'])
def validate_tx():
    pass




from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()