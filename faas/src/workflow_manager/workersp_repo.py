from gevent import monkey
monkey.patch_all()
from typing import List, Any
import couchdb
import boto3
import sys

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

    def get_all_data_from_db(self):
        response = self.table.scan()
        return response['Items']

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

    def store_data_to_db(self, key, version, value):
        self.table.put_item(
            Item={
                'key': key,
                'version': version,
                'value': value
            }
        )

class Repository:
    def __init__(self):
        self.dynamo = boto3.resource('dynamodb', endpoint_url=dynamodb_url, aws_secret_access_key=dynamodb_access_key, aws_access_key_id=dynamodb_key_id, region_name=dynamodb_area)
        self.data_db = self.dynamo.Table('data')
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
            
    def get_end_function(self, db_name) -> str:
        db = self.couch[db_name]
        for item in db:
            doc = db[item]
            if 'end_function' in doc:
                return doc['end_function']['name']
            

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

    def clear_db(self, transaction_id):
        db = self.couch['results']
        db.delete(db[transaction_id])

    def log_status(self, workflow_name, transaction_id, status):
        log_db = self.couch['log']
        log_db.save({'transaction_id': transaction_id, 'workflow': workflow_name, 'status': status})
    
    def save_latency(self, log):
        latency_db = self.couch['workflow_latency']
        latency_db.save(log)

    def param_wrapper(self, transaction_id, mode, func="" ,key=""):
        return f"{transaction_id}:{mode}:{func}:{key}" 
    
    def param_decode(self, redis_key):
        parts = redis_key.split(':')
        key = ':'.join(parts[3:])  # 将索引3及之后的部分重新组合成key
        return key

    def release_lock(self, transaction_id, lock_set):
        """
        释放 lock_set 中每个 key 的锁，将其 lock 属性设置为 None。
        """

        for key in lock_set.keys():
            # 更新 lock 属性为 None
            self.data_db.update_item(
                Key={'key': key},
                UpdateExpression="SET #l = :none",
                ExpressionAttributeNames={
                    '#l': 'lock'
                },
                ConditionExpression="#l = :txid",  # 确保当前锁属于 transaction_id
                ExpressionAttributeValues={
                    ':txid': transaction_id,
                    ':none': None
                },
                ReturnValues="UPDATED_NEW"
            )

    def sync_shadow_to_data_db_with_version(self, transaction_id, version=''):
        shadow_table_name = f"{transaction_id}_shadow_table"
        shadow_table = self.dynamo.Table(shadow_table_name)

        # 扫描 shadow table 中的所有数据
        response = shadow_table.scan()
        items = response.get('Items', [])

        for item in items:
            key = item['key']
            value = item['value']  # Ensure value is stored as a string
            # only flush the items func write.
            if not key.startswith('RET'):
                self.data_db.update_item(
                    Key={'key': key},
                    UpdateExpression="SET #v = :value, #ver = :version",
                    ExpressionAttributeNames={
                        '#v': 'value',
                        '#ver': 'version'
                    },
                    ExpressionAttributeValues={
                        ':value': value,
                        ':version': version
                    },
                    ReturnValues="UPDATED_NEW"
                )

    def beldi_commit(self, transaction_id, lock_set):
        self.sync_shadow_to_data_db_with_version(transaction_id)
        self.release_lock(transaction_id, lock_set)
