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

    def flush_couchdb_workflow_latency(self):
        if 'workflow_latency' in self.couch:
            self.couch.delete('workflow_latency')
        db = self.couch.create('workflow_latency')
        db = self.couch['workflow_latency']
        time.sleep(1)

    def get_all_latencies_for_txs(self, txids: list) -> dict:
        """
        根据一个事务ID列表，使用单次高效的Mango查询批量获取io, exec, 和 lock 延迟。
        返回一个字典，键是 transaction_id，值是包含各类延迟总和的字典。
        """
        if not txids:
            return {}
            
        # 初始化结果字典，为每个txid设置默认延迟
        latencies = {txid: defaultdict(float) for txid in txids}
        try:
            db = self.couch['workflow_latency']
            print(f"success txs len:{len(txids)}")
            # 获取数据库中的文档总数，设置查询限制
            total_docs = len(db)
            query_limit = max(total_docs + 100, len(txids) * 3 + 100)
            # 查询 io, exec, 和 lock 三种类型的延迟
            query = {
                'selector': {
                    'transaction_id': {'$in': txids},
                    'phase': {'$in': ['io', 'exec', 'lock']}
                },
                'fields': ['transaction_id', 'phase', 'time'],
                'limit': query_limit  # <--- 在这里添加 limit
            }
            results = db.find(query)
            for doc in results:
                tx_id = doc.get('transaction_id')
                phase = doc.get('phase')
                if tx_id and phase:
                    latencies[tx_id][f'{phase}_latency'] += doc.get('time', 0)
        except Exception as e:
            print(f"Error during bulk fetching latencies: {e}", file=sys.stderr)
        
        # 将 defaultdict 转换为普通 dict
        return {txid: dict(data) for txid, data in latencies.items()}

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
    
    def get_all_latencies(self):
        db = self.couch['workflow_latency']
        for item in db:
            doc = db[item]
            if 'transaction_id' in doc:
                print(doc)
                print('----------------------')


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

if __name__ == '__main__':
    repo = Repository()
    latency = repo.get_all_latencies()
    print(latency)
