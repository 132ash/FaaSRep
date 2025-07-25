import sys
import logging
# 配置日志记录
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    # 设置日志级别为 INFO
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',  # 日志格式
    datefmt='%Y-%m-%d %H:%M:%S',  # 设置日期格式
    handlers=[
        logging.StreamHandler(sys.stdout)  # 将日志输出到标准输出
    ],
    force=True 
)


from gevent import monkey
monkey.patch_all()
import json
from typing import Dict
sys.path.append('../../config')
import config
from concord_repo import Repository
from flask import Flask, request
app = Flask(__name__)
repo = Repository()
from Concord_cache_agent import ConcordCacheAgent

sys.path.append('../../config')
import config

class ConcordDispatcher:
    def __init__(self, info_addrs: Dict[str, str]) -> None:
        self.host_addr = sys.argv[1] + ':' + sys.argv[2]
        self.node_list = repo.get_all_addrs('common')
        self.concord_cache_agent = {name:ConcordCacheAgent(name, repo, self.node_list, sys.argv[1])  for name in info_addrs}

dispatcher = ConcordDispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)

@app.route('/clear_state', methods = ['POST'])
def clear_state():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    workflow = data['workflow_name']
    dispatcher.concord_cache_agent[workflow].clean_access_set_of_tx(transaction_id)
    return {'status': 'success'}

@app.route('/concord_data', methods = ['POST'])
def concord_data():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    key = data['key']
    workflow = data['workflow']
    trigger_tx = data.get('trigger_tx', '')
    success = True
    value = ''
    if mode == 'invalidated':
        success = dispatcher.concord_cache_agent[workflow].invalidated_by_home(key, trigger_tx)
    elif mode == "downgrade":
        success, value = dispatcher.concord_cache_agent[workflow].downgrade_by_home(key)
    else:
        success, value = dispatcher.concord_cache_agent[workflow].data_access(trigger_tx, key, mode)
    return {'success':success, 'value': value}
    
@app.route('/concord_home', methods = ['POST'])
def concord_home():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    remote_ip = data['remote_ip']
    key = data['key']
    workflow = data['workflow']
    transaction_id = data['transaction_id']
    value = ''
    if mode == 'read':
        success, value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_read(transaction_id, key, remote_ip)
    else:
        success, value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_write(transaction_id, key, remote_ip, mode)
    return {'success':success, 'value': value, 'state': state}

# python proxy.py  10.2.27.24 6000
# python proxy.py  10.2.30.52 6000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   