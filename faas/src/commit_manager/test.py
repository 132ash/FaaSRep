from gevent import monkey
monkey.patch_all()
from validator_repo import Repository
import requests
import time
import gevent 

repo = Repository()
print(repo.get_initial_data_version())
print(repo.get_start_functions('testflow_workflow_metadata'))
print(repo.get_all_addrs('testflow_workflow_metadata'))

def send_validation_request(data):
    txid = data["transaction_id"]
    print(f"sending to txid:{txid}")
    requests.post(validator_url, json=data)

read_set = {"func1":{'test_value':'0:-1'}, "func2_1":{'test_value':'0:-1'}, "func2_2":{'test_value':'0:-1'}, "func3":{'test_value':'0:-1'}}
write_set =  {"test_value": {"ip":"192.168.162.130:7000", "func":"func3"}}
function_pos = { "func1": "192.168.162.130:7000",
                 "func2_1": "192.168.162.131:7000",
                 "func2_2": "192.168.162.130:7000",
                 "func3": "192.168.162.131:7000" }

validator_url =  "http://192.168.162.132:9000/validate"
data = {"read_set": read_set, "write_set": write_set, "workflow_name": "testflow", "transaction_id": "test_tx_id", "function_pos": function_pos}

jobs = []

for tx_id in range(10):
    data1 = data.copy()
    data1["transaction_id"] = f"test_tx_id_{tx_id}"
    jobs.append(gevent.spawn(send_validation_request, data1))
gevent.joinall(jobs)

