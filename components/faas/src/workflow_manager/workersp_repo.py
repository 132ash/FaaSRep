from typing import Any, List
import couchdb
import redis
import threading
import sys

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

class Repository:
    def __init__(self):
        self.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        self.couch = couchdb.Server(couchdb_url)

    # get all function_name for every node seems to solve the problem of KeyError Exception in manager.py, line 103
    def get_current_node_functions(self, ip: str, mode: str) -> List[str]:
        db = self.couch[mode]
        functions = []
        for item in db:
            functions.append(db[item]['function_name'])
        return functions

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions']
            
    def get_end_function(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'end_function' in doc:
                return doc['end_function']   
            

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.couch[mode]
        for item in db.find({'selector': {'function_name': function_name}}):
            return item
    
    def clear_mem(self, transaction_id):
        keys = self.redis.keys()
        for key in keys:
            key_str = key.decode()
            if key_str.startswith(transaction_id):
                self.redis.delete(key)

    def clear_db(self, transaction_id):
        db = self.couch['results']
        db.delete(db[transaction_id])

    def log_status(self, workflow_name, transaction_id, status):
        log_db = self.couch['log']
        log_db.save({'transaction_id': transaction_id, 'workflow': workflow_name, 'status': status})
    
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)

    def save_tx_result(self, transaction_id, db_name):
        end_function = self.get_end_function(db_name)
        result = self.fetch_result(transaction_id, end_function["name"], end_function["output"])
        db = self.couch['results']
        try:
            # 尝试获取现有文档
            doc = db[transaction_id]
            # 如果存在，更新文档
            doc['result'] = result
            db.save(doc)
        except couchdb.http.ResourceNotFound:
            # 如果文档不存在，创建新文档
            doc = {
                '_id': transaction_id,
                'result': result
            }
            db.save(doc)

    def param_wrapper(self, transaction_id, func ,key):
        return f"{transaction_id}:{func}:{key}" 

    # input_keys: specify the keys you want
    def fetch_result(self, transaction_id, func, output):
        keys = output.keys()
        result = {}
        for k in keys:
            redis_key = self.param_wrapper(transaction_id, func, k)
            if output[k]["type"] == "int":
                result[k] = int(self.redis[redis_key])
            else:
                result[k] = self.redis[redis_key]
        return result
    
    def store_input(self, input):
        for k, v in input.items():
            redis_key = self.param_wrapper("GLOBAL", k)
            self.redis[redis_key] = v