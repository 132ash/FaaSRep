import couchdb
import sys
from pathlib import Path
experiment_dir = Path(__file__).parent.parent
sys.path.append(str(experiment_dir.parent / 'config'))
import config

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(config.COUCHDB_URL)


    def get_latencies(self):
        latencies = []
        for _id in self.couch['workflow_latency']:
            doc = self.couch['workflow_latency'][_id]
            transaction_id = doc['transaction_id']
            print(f"Transaction ID: {transaction_id}")
            for key, value in doc.items():
                print(f"{key}: {value}")

if __name__ == "__main__":
    repo = Repository()
    repo.get_latencies()
