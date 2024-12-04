from typing import Any, List
import couchdb
import redis
import json
import sys

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

# interact with couchdb node

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        self.redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)

    def get_start_functions(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'start_functions' in doc:
                return doc['start_functions']

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
            
    def get_tx_result(self, transaction_id):
        db = self.couch["results"]
        try:
            doc = db[transaction_id]
            return doc['result']
        except couchdb.http.ResourceNotFound:
            return None

    def get_result(self, request_id: str) -> Any:
        result = dict()
        doc = self.couch['results'][request_id]
        for k in doc:
            result[k] = doc[k]
        return result

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.couch[mode]
        for item in db.find({'selector': {'function_name': function_name}}):
            return item


    def create_request_doc(self, request_id: str) -> None:
        if request_id in self.couch['results']:
            doc = self.couch['results'][request_id]
            self.couch['results'].delete(doc)
        self.couch['results'][request_id] = {}
    
    def param_wrapper(self, transaction_id, func ,key):
        return f"{transaction_id}:{func}:{key}" 
    
    def store_input(self, transaction_id, input):
        for k, v in input.items():
            redis_key =  f"{transaction_id}:GLOBAL:{k}"
            self.redis[redis_key] = v
