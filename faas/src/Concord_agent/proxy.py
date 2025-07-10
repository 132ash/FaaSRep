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
        self.concord_cache_agent = {name:ConcordCacheAgent(name, repo, self.host_addr)  for name in info_addrs}

dispatcher = ConcordDispatcher(info_addrs=config.FUNCTION_INFO_ADDRS)

@app.route('/concord_data', methods = ['POST'])
def concord_data():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    key = data['key']
    workflow = data['workflow']
    trigger_tx = data['trigger_tx']
    value = ''
    if mode == 'invalidated':
        dispatcher.concord_cache_agent[workflow].invalidate_by_home(key, trigger_tx)
    else:
        value = dispatcher.concord_cache_agent[workflow].data_access(trigger_tx, key, mode)
    return json.dumps({'value': value})

@app.route('/concord_lock', methods = ['POST'])
def concord_lock():
    data = request.get_json(force=True, silent=True)
    transaction_id = data['transaction_id']
    lock_keys = data['lock_keys']
    lock = data['lock']  # default to True, meaning acquire locks
    dispatcher.concord_cache_agent[data['workflow']].lock_or_unlock_for_commit(transaction_id, lock_keys, lock)
    
@app.route('/concord_home', methods = ['POST'])
def concord_home():
    data = request.get_json(force=True, silent=True)
    mode = data['mode']
    remote_ip = data['remote_ip']
    key = data['key']
    workflow = data['workflow']
    transaction_id = data['transaction_id']
    value = ''
    if mode == 'invalidated':
        # invalidate the cache on this node.
        value, state = dispatcher.concord_cache_agent[workflow].invalidate_by_home(key, transaction_id)
    if mode == 'read':
        value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_read(transaction_id, key, remote_ip)
    else:
        value, state = dispatcher.concord_cache_agent[workflow].home_serve_remote_write(transaction_id, key, remote_ip, mode)
    return json.dumps({'value': value, 'state': state})

# python proxy.py  192.168.162.130 6000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
   