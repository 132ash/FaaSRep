from gevent import monkey
monkey.patch_all()
from typing import Dict, List, Any
import couchdb
import redis
import boto3
from datetime import datetime
import sys
import json

sys.path.append('../../config')
import config

couchdb_url = config.COUCHDB_URL
dynamodb_url = config.DYNAMODB_URL
dynamodb_key_id = config.DYNAMODB_KEY_ID
dynamodb_access_key = config.DYNAMODB_ACCESS_KEY
dynamodb_area = config.DYNAMODB_AREA

class DynamoDBClient:
    def __init__(self, endpoint_url, aws_secret_access_key, aws_access_key_id, region_name):
        self.client = boto3.resource('dynamodb', endpoint_url=endpoint_url, aws_secret_access_key=aws_secret_access_key, aws_access_key_id=aws_access_key_id, region_name=region_name)
        self.table = self.client.Table('data')


    def get_data_from_db(self, key):
        # 从dynamodb中获取数据
        response = self.table.get_item(
            Key={
                'key': key
            }
        )
        item = response.get('Item')
        if item:
            return item['version'], item['value']
        else:
            return None, None
        
class Repository:
    def __init__(self):
        self.cache_redis = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.CACHE_DB)
        self.data_db = DynamoDBClient(dynamodb_url, dynamodb_access_key, dynamodb_key_id, dynamodb_area)
        self.couch = couchdb.Server(couchdb_url)
        self.shadowtable_redis_all_addr:Dict[str, redis.StrictRedis] =  {
                    host:redis.StrictRedis(host=host, port=config.REDIS_PORT, db=config.SHADOWTABLE_DB, decode_responses=True)
                    for host in self.get_all_addrs('common')
                    }

    def get_all_addrs(self, db_name) -> List[str]:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'addrs' in doc:
                return doc['addrs']
