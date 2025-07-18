from redis_component import RedisShadowTable
import container_config
import redis
import requests
import json
import logging

class FaaSTCCStore:
    def __init__(self, workflow, validator_addr,redis_shadow_table:RedisShadowTable, function_pos, db_server):
        self.workflow = workflow
        self.validator_addr = validator_addr
        self.function_pos = function_pos
        self.data_db = db_server.Table('data')
        self.shadow_table:RedisShadowTable = redis_shadow_table
        self.snapshot_interval = []
        self.cache = redis.StrictRedis(host=container_config.CACHE_HOST, port=container_config.REDIS_PORT, db=container_config.REDIS_CACHE_DB, decode_responses=True)
        
    def runtime_init(self, transaction_id ,snapshot_interval, read_set):
        self.transaction_id = transaction_id
        self.snapshot_interval = snapshot_interval
        self.read_set = read_set

    def param_wrapper(self, func , key, mode, txid=None):
        if txid:
            return f"{txid}:{mode}:{func}:{key}" 
        else:
            return f"{self.transaction_id}:{mode}:{func}:{key}" 

    def get(self, key, upstream_func):
        if upstream_func:
            upstrseam_ip = self.function_pos[upstream_func]
            return self.shadow_table.raw_fetch_data(self.param_wrapper(upstream_func, key, 'PUT'), upstrseam_ip)
        elif key in self.read_set:
            return self.read_set[key]
        else:
            value = self.get_from_cache(key)
            self.read_set[key] = value
            return value

    def get_from_cache(self, key):
        value_tuple = self.cache.get(key)
        if value_tuple == None:
            value, version, promise = self.get_from_storage(key, self.snapshot_interval[1])
            self.update_cache(key, value, version, promise)
        else:
            value_tuple = json.loads(value_tuple)
            value = value_tuple['value']
            version = value_tuple['version']
            promise = value_tuple['promise']
            if version > self.snapshot_interval[1]:
                value, version, promise = self.get_from_storage(key, self.snapshot_interval[1])
            elif promise < self.snapshot_interval[0]:
                value, version, promise = self.get_from_storage(key, self.snapshot_interval[1])
                self.update_cache(key, value, version, promise)
        self.snapshot_interval[0] = max(self.snapshot_interval[0], version)
        self.snapshot_interval[1] = min(self.snapshot_interval[1], promise)
        logging.info(f"[{key}] get version {version}, promise {promise}, update snapshot interval: {self.snapshot_interval}")
        return value
    
    def get_from_storage(self, key, version):
        url = f"http://{self.validator_addr}/FaaSTCC_get"
        data = {'workflow_name':self.workflow,'key': key, 'version':self.snapshot_interval[1]}
        response = requests.post(url, json=data).json()
        version = response.get('version', '')
        if not version:
            raise Exception("Failed to get data from FaaSTCC storage layer. Abort.")
        version = response['version']
        promise = response['promise']
        response = self.data_db.get_item(
            Key={
                'key': key,
                'version': version
            }
        )
        item = response.get('Item')
        value = item['value']
        return value, version, promise
    
    def update_cache(self, key, value, version, promise):
        data = {'value': value, 'version': version, 'promise': promise}
        self.cache.set(key, json.dumps(data))
                 