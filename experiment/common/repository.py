import couchdb
import gevent
import redis
import sys
import requests
from pathlib import Path
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
