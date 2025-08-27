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

    def get_latencies_for_txs_by_phase(self, txids: list, phase: str) -> dict:
        """
        根据一个事务ID列表和指定的阶段(phase)，使用单次高效的Mango查询批量获取并加总延迟。
        返回一个字典，键是 transaction_id，值是对应的总延迟。
        """
        if not txids:
            return {}
            
        latencies = defaultdict(float)
        try:
            db = self.couch['workflow_latency']
            # 使用 $in 操作符进行批量查询
            query = {
                'selector': {
                    'transaction_id': {'$in': txids},
                    'phase': phase
                },
                'fields': ['transaction_id', 'time'],
                'limit': len(txids) * 10 # 设置一个足够大的限制以获取所有相关文档
            }
            results = db.find(query)
            for doc in results:
                tx_id = doc.get('transaction_id')
                if tx_id:
                    latencies[tx_id] += doc.get('time', 0)
        except Exception as e:
            print(f"Error during bulk fetching {phase.upper()} latencies: {e}", file=sys.stderr)
        return dict(latencies)

    def get_latencies(self):
        db = self.couch['workflow_latency']
        map_fun = '''function(doc) {
            if (doc.phase === 'io' || doc.phase === 'exec') {
                emit(doc.transaction_id, {time: doc.time, phase: doc.phase});
            }
        }'''
        
        reduce_fun = '''function(keys, values, rereduce) {
            var result = {io_sum: 0, exec_sum: 0};
            if (rereduce) {
                for (var i = 0; i < values.length; i++) {
                    result.io_sum += values[i].io_sum;
                    result.exec_sum += values[i].exec_sum;
                }
            } else {
                for (var i = 0; i < values.length; i++) {
                    if (values[i].phase === 'io') {
                        result.io_sum += values[i].time;
                    } else if (values[i].phase === 'exec') {
                        result.exec_sum += values[i].time;
                    }
                }
            }
            return result;
        }'''
        
        results = db.query(map_fun, reduce_fun, group=True)
        
        latencies = {}
        for row in results:
            latencies[row.key] = {
                'io_latency': row.value['io_sum'],
                'exec_latency': row.value['exec_sum']
            }
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