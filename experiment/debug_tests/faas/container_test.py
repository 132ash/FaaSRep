import requests
import os


os.system('docker rm -f $(docker ps -aq --filter label=workflow)')
base_url = 'http://127.0.0.1:{}/{}'

input = {"chained_num_0":{ "from": "GLOBAL", "type": "int", "value": 0}}    
output = {"chained_num_1":{"type": "int"}}
data = {"transaction_id":1, "input":input, "output":output}


r = requests.post(base_url.format(20000, 'run'), json=data)
print(r.json())

# docker run -p 5000:20000 --label workflow testflow_func1
# docker rm -f $(docker ps -aq --filter label=workflow)