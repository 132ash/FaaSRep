from flask import Flask, jsonify
import threading
import sys
import os

# 通过参数或环境变量切换模式
mode = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SERVER_MODE", "thread")

if mode == "gevent":
    from gevent import monkey
    monkey.patch_all()

app = Flask(__name__)

class SharedData:
    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

shared_data = SharedData()

def fib(n):
    if n <= 1:
        return n
    return fib(n-1) + fib(n-2)

@app.route('/test', methods=['GET'])
def test():
    with shared_data.lock:
        before = shared_data.value
        shared_data.value += 1
        after = shared_data.value
    fib(35)
    return jsonify({"before": before, "after": after})

if __name__ == '__main__':
    if mode == "gevent":
        # gunicorn -k gevent -w 1 -b 0.0.0.0:9000 server:app
        from gevent.pywsgi import WSGIServer
        print("Running in gevent (协程)模式")
        server = WSGIServer(('0.0.0.0', 9000), app)
        server.serve_forever()
    else:
        # gunicorn -w 1 --threads 8 -b 0.0.0.0:9000 server:app
        print("Running in threading (线程)模式")
        app.run(host='0.0.0.0', port=9000, threaded=True)