from gevent import monkey
monkey.patch_all()
from flask import Flask, request
import sys
import logging
from Concord_app_controller import AppControllerConcord
import json
app = Flask(__name__)

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

sys.path.append('../../config')
import config

GATEWAY_ADDR = config.GATEWAY_ADDR
VALIDATE = 1
COMMIT = 2
PESSIMISTIC_REPAIR_FINISH = 4

workflows = config.FUNCTION_INFO_ADDRS.keys()
App_Controller_Concord = {workflow: AppControllerConcord(workflow) for workflow in workflows}
    
@app.route('/commit', methods = ['POST'])
def transaction_commit():
    data = request.get_json(force=True, silent=True)
    workflow = data['workflow_name']
    transaction_id = data['transaction_id']
    read_set = data['read_set']
    write_set = data['write_set']
    App_Controller_Concord[workflow].commit(transaction_id, read_set, write_set)
    return json.dumps({'status': 'successed'})

@app.route('/concord_abort', methods = ['POST'])
def concord_abort():
    data = request.get_json(force=True, silent=True)
    workflow_name = data['workflow_name']
    transaction_id = data['transaction_id']
    App_Controller_Concord[workflow_name].abort(transaction_id)
    return json.dumps({'status': 'aborted'})

# python proxy.py 10.2.27.24 9000
from gevent.pywsgi import WSGIServer
import logging
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S', level='INFO')
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()