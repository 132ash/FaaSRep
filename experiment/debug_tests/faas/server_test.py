import requests
import couchdb
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
sys.path.append(str(ROOT_DIR))
import config.config as config

COUCHDB_URL = config.COUCHDB_URL

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
