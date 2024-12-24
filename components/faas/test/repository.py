import couchdb
import sys
sys.path.append('../config')
import config

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(config.COUCHDB_URL)

    def flush_couchdb_workflow_latency(self):
        self.couch.delete('workflow_latency')
        self.couch.create('workflow_latency')

    def get_latencies(self, txid, phase):
        latencies = []
        for _id in self.couch['workflow_latency']:
            doc = self.couch['workflow_latency'][_id]
            if doc['transaction_id'] == txid and doc['phase'] == phase:
                latencies.append(doc['time'])
        return latencies

