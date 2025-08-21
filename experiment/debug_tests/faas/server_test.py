import requests
import couchdb
STORAGE_NODE_IP = '10.2.64.4'

COUCHDB_URL = f'http://faasnap:faasnap@{STORAGE_NODE_IP}:5984'

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(COUCHDB_URL)

    def flush_couchdb_workflow_latency(self):
        if 'workflow_latency' in self.couch:
            self.couch.delete('workflow_latency')
        db = self.couch.create('workflow_latency')
        # 确保创建成功
        assert db is not None

    def get_latencies(self):
        for _id in self.couch['workflow_latency']:
            doc = self.couch['workflow_latency'][_id]
            print(doc['transaction_id'], doc['phase'], doc['time'])
    
repo = Repository()
repo.get_latencies()
