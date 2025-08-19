import couchdb
import gevent
import redis
import sys
import requests
from pathlib import Path
from collections import defaultdict
import time

experiment_dir = Path(__file__).parent.parent
sys.path.append(str(experiment_dir.parent))
import config.config as config

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(config.COUCHDB_URL)
        self.all_addrs = self.get_all_addrs()
        self.shadowtable_redis_all_addr =  {
                    host:redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB, decode_responses=True)
                    for host in self.all_addrs
                    }
        self.cache_all_addrs = {
            host:redis.StrictRedis(host=host, port=config.REDIS_CACHE_PORT, db=config.CACHE_DB, decode_responses=True)
            for host in self.all_addrs
        }

    def flush_couchdb_workflow_latency(self):
        if 'workflow_latency' in self.couch:
            self.couch.delete('workflow_latency')
        db = self.couch.create('workflow_latency')
        db = self.couch['workflow_latency']
        time.sleep(1)

    def get_io_latencies_for_txs(self, txids: list) -> dict:
        if not txids:
            return {}

        io_latencies = defaultdict(float)
        print("getting io latency")
        try:
            db = self.couch['workflow_latency']
            # 使用 $in 操作符进行批量查询
            query = {
                'selector': {
                    'transaction_id': {'$in': txids},
                    'phase': 'io'
                },
                'fields': ['transaction_id', 'time'],
                'limit': len(txids) * 100 # 设置一个足够大的限制以获取所有相关文档
            }
            results = db.find(query)
            for doc in results:
                tx_id = doc.get('transaction_id')
                if tx_id:
                    io_latencies[tx_id] += doc.get('time', 0)
        except Exception as e:
            print(f"Error during bulk fetching IO latencies: {e}", file=sys.stderr)
        return dict(io_latencies)

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

    def clear_all_memory_and_container(self):
        for shadow_table in self.shadowtable_redis_all_addr.values():
            shadow_table.flushall(True)
        for cache in self.cache_all_addrs.values():
            cache.flushall(True)
        clear_container_jobs = []
        for worker_sp_ip in self.all_addrs:
            url = f'http://{worker_sp_ip}:7500/clear_container'
            clear_container_jobs.append(gevent.spawn(requests.post, url, json={}))
        gevent.joinall(clear_container_jobs)
