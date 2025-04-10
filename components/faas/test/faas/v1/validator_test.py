import requests
import threading
from datetime import datetime


startup_version = datetime(2000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')
VALIDATOR_ADDR = 'http://192.168.162.132:9000/validate'

# 转换后的测试数据 1
batch1 = {
    "batch": {
        "batch_id": "tx1",
        "workflow_name": {"tx1": "workflow_1"},
        "read_set": {"tx1": {"func1": {"test_value": startup_version}}},
        "write_set": {"tx1": {"test_value": "func1"}},
        "RYW_subjection": {"tx1": {}},
        "function_pos": {"tx1": {"func1": {"ip": "192.168.1.1", "port": 5000}}},
        "worker_set": {"192.168.1.1": "worker_1"},
        "transaction_list": ["tx1"]
    }
}

# 转换后的测试数据 2
batch2 = {
    "batch": {
        "batch_id": "tx2",
        "workflow_name": {"tx2": "workflow_2"},
        "read_set": {"tx2": {"func2": {"test_value": startup_version}}},
        "write_set": {"tx2": {"test_value": "func2"}},
        "RYW_subjection": {"tx2": {}},
        "function_pos": {"tx2": {"func2": {"ip": "192.168.1.2", "port": 5001}}},
        "worker_set": {"192.168.1.2": "worker_2"},
        "transaction_list": ["tx2"]
    }
}

def send_request(data):
    try:
        response = requests.post(VALIDATOR_ADDR, json=data)
        print(f"Response for batch {data['batch']['batch_id']}: {response.status_code}, {response.json()}")
    except Exception as e:
        print(f"Error sending request for batch {data['batch']['batch_id']}: {e}")

# 创建线程
thread1 = threading.Thread(target=send_request, args=(batch1,))
thread2 = threading.Thread(target=send_request, args=(batch2,))

# 启动线程
thread1.start()
thread2.start()

# 等待线程完成
thread1.join()
thread2.join()