
import requests
url = "http://10.2.27.22:8000/run"
data = {'workflow':'test', "parameters":{}}
rep = requests.post(url, json=data)