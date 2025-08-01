import couchdb
import sys
from pathlib import Path
experiment_dir = Path(__file__).parent
sys.path.append(str(experiment_dir.parent / 'config'))
import config

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(config.COUCHDB_URL)

    def flush_couchdb_workflow_latency(self):
        if 'workflow_latency' in self.couch:
            self.couch.delete('workflow_latency')
        db = self.couch.create('workflow_latency')
        # 确保创建成功
        assert db is not None

    def get_latencies(self, txid, phase):
        latencies = []
        for _id in self.couch['workflow_latency']:
            doc = self.couch['workflow_latency'][_id]
            if doc['transaction_id'] == txid and doc['phase'] == phase:
                latencies.append(doc['time'])
        return latencies
    
    def get_all_addrs(self):
        db = self.couch['common']
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
    
    def get_all_functions(self, workflow_name):
        db = self.couch[f"{workflow_name}_function_info"]
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions

