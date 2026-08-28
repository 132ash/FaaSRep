from typing import Any, List
import couchdb
import redis
import boto3
import sys
import re
import logging

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
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

# interact with couchdb node

class Repository:
    def __init__(self):
        self.couch = couchdb.Server(couchdb_url)
        addrs = self.get_all_addrs('common')
        self.dynamo = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)
        
        self.redis = {
            host : redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB)
                for host in addrs
            }
 

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
            
    def get_tx_result(self, transaction_id):
        db = self.couch["results"]
        try:
            doc = db[transaction_id]
            return doc['result']
        except couchdb.http.ResourceNotFound:
            return None

    def get_function_info(self, function_name: str, mode: str) -> Any:
        db = self.couch[mode]
        for item in db.find({'selector': {'function_name': function_name}}):
            return item

    def get_function_addrs(self, workflow_name: str) -> List[str]:
        """Return only workers that host a function in this workflow."""
        db = self.couch[workflow_name + '_function_info']
        return list(dict.fromkeys(
            doc['ip'] for doc_id in db
            for doc in [db[doc_id]]
            if doc.get('function_name') and doc.get('ip')
        ))

    def create_request_doc(self, request_id: str) -> None:
        if request_id in self.couch['results']:
            doc = self.couch['results'][request_id]
            self.couch['results'].delete(doc)
        self.couch['results'][request_id] = {}

    def param_wrapper(self, transaction_id, mode, func ,key, is_dynamo):
        if is_dynamo:
            return f"{mode}:{func}:{key}"
        else:
            return f"{transaction_id}:{mode}:{func}:{key}" 
    
    def store_input(self, transaction_id, ip, input):
        for k, v in input.items():
            redis_key = self.param_wrapper(transaction_id, 'RET','GLOBAL', k, False)
            self.redis[extract_ip(ip)][redis_key] = v

    def get_result(self, request_id: str, workflow_name) -> Any:
        end_func = self.get_end_function(workflow_name + '_workflow_metadata')
        end_func_name = end_func['name']
        info = self.get_function_info(end_func['name'], workflow_name + '_function_info')
        ip = extract_ip(info['ip'])
        output = end_func['output']
        return self.fetch_result_from_shadow_table(request_id, end_func_name, output, ip)

    def fetch_result_from_shadow_table(self, transaction_id, func, output, redis_ip):
        keys = output.keys()
        result = {}
        for k in keys:
            redis_key = self.param_wrapper(transaction_id, 'RET', func, k, False)
            result[k] = int(self.redis[redis_ip][redis_key].decode('utf-8')) if output[k]["type"] == "int" else self.redis[redis_ip][redis_key].decode('utf-8')
        return result
