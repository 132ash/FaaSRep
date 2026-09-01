from gevent import monkey
monkey.patch_all()
from flask import Flask, jsonify, request
import sys
from pathlib import Path

from shadow_store import ShadowStore

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
import config
from db_repo import DynamoDBRepository

db = DynamoDBRepository(config.DYNAMODB_URL, config.DYNAMODB_KEY_ID,
                        config.DYNAMODB_ACCESS_KEY, config.DYNAMODB_AREA)
store = ShadowStore(db)
app = Flask(__name__)


def body():
    return request.get_json(force=True, silent=False)


@app.route('/begin', methods=['POST'])
def begin():
    data = body()
    return jsonify(store.begin(data['txid'], int(data.get('term', 0)), int(data['birth_seq'])))


@app.route('/get', methods=['POST'])
def get():
    data = body()
    return jsonify(store.get(data['txid'], int(data['term']), data['key']))


@app.route('/put', methods=['POST'])
def put():
    data = body()
    return jsonify(store.put(data['txid'], int(data['term']), data['key'], data['value'],
                              data.get('function', ''), data['op_id']))


@app.route('/flush', methods=['POST'])
def flush():
    data = body()
    return jsonify(store.flush(data['txid'], int(data['term']), data['flush_id']))


@app.route('/discard', methods=['POST'])
def discard():
    data = body()
    return jsonify(store.discard(data['txid'], int(data['term']), data.get('reason', 'ERROR')))


@app.route('/complete', methods=['POST'])
def complete():
    data = body()
    return jsonify(store.complete(data['txid'], int(data['term'])))


@app.route('/debug/tx/<txid>', methods=['GET'])
def debug(txid):
    return jsonify(store.debug_tx(txid))


@app.route('/health', methods=['GET'])
def health():
    return jsonify(store.health())

# python3 proxy.py 10.2.29.142 9100
if __name__ == '__main__':
    from gevent.pywsgi import WSGIServer
    server = WSGIServer((sys.argv[1], int(sys.argv[2])), app)
    server.serve_forever()
