import couchdb
import sys
from pathlib import Path
import time

experiment_dir = Path(__file__).parent.parent
sys.path.append(str(experiment_dir.parent))
import config.config as config

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(config.COUCHDB_URL)

    def flush_couchdb_workflow_latency(self):
        if 'workflow_latency' in self.couch:
            self.couch.delete('workflow_latency')
        db = self.couch.create('workflow_latency')
        db = self.couch['workflow_latency']
        time.sleep(1)

    def get_latencies(self):
        latencies = {}
        # print(f"Fetching latencies for txid: {txid}, phase: {phase}")
        db = self.couch['workflow_latency']
        for _id in db:
            doc = db[_id]
            latencies.setdefault(doc['workflow_name'], {}).setdefault(doc['phase'], []).append(doc['time'])
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

