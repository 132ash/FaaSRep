from typing import Any, List
import couchdb
import redis
import json
import sys
import re

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL

# interact with couchdb node

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        addrs = self.get_all_addrs('common')
        self.redis = {
            host : redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.REDIS_DB)
                for host in addrs
            }
 

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
    
    def store_input(self, transaction_id, ip, input):
        for k, v in input.items():
            redis_key =  f"{transaction_id}:RET:GLOBAL:{k}"
            self.redis[extract_ip(ip)][redis_key] = v
            print(self.redis[extract_ip(ip)][redis_key])
