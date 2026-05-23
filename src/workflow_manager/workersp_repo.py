from gevent import monkey
monkey.patch_all()
from typing import List, Any
import couchdb
import boto3
import sys
import logging
import gevent
from gevent import queue

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
        self.collect_function_latency = getattr(config, 'COLLECT_FUNCTION_LATENCY', False)
        self.latency_batch_size = getattr(config, 'LATENCY_BATCH_SIZE', 128)
        self.latency_flush_interval = getattr(config, 'LATENCY_FLUSH_INTERVAL', 0.05)
        self.latency_queue = queue.Queue()
        if self.collect_function_latency:
            gevent.spawn(self._latency_writer_loop)
        
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
        if self.collect_function_latency:
            self.latency_queue.put(log)

    def save_latencies(self, logs):
        if not self.collect_function_latency:
            return
        for log in logs:
            self.latency_queue.put(log)

    def _latency_writer_loop(self):
        pending = []
        while True:
            try:
                pending.append(self.latency_queue.get(timeout=self.latency_flush_interval))
            except queue.Empty:
                pass

            while len(pending) < self.latency_batch_size:
                try:
                    pending.append(self.latency_queue.get_nowait())
                except queue.Empty:
                    break

            if not pending:
                continue

            batch = pending
            pending = []
            try:
                latency_db = self.couch['workflow_latency']
                latency_db.update(batch)
            except Exception as exc:
                print(f"Failed to write latency batch to CouchDB: {exc}", file=sys.stderr)

    def param_wrapper(self, transaction_id, mode, func="" ,key=""):
        return f"{transaction_id}:{mode}:{func}:{key}" 
    
    def param_decode(self, redis_key):
        parts = redis_key.split(':')
        key = ':'.join(parts[3:])  # 将索引3及之后的部分重新组合成key
        return key

    def sync_shadow_to_data_db_with_version(self, transaction_id, version=''):
        # 关键修改：使用全局的 shadow_table
        shadow_table = self.dynamo.Table('shadow_table')

        # 关键修改：使用 query 代替 scan，高效查询属于该事务的所有条目
        response = shadow_table.query(
            KeyConditionExpression='txid = :txid',
            ExpressionAttributeValues={':txid': transaction_id}
        )
        items = response.get('Items', [])

        # 处理分页，以防一个事务的写入集超过1MB
        while 'LastEvaluatedKey' in response:
            response = shadow_table.query(
                KeyConditionExpression='txid = :txid',
                ExpressionAttributeValues={':txid': transaction_id},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        for item in items:
            # shadow_table 中的 'key' 字段现在是排序键
            key = item['key']
            value = item['value']
            
            # 只将函数写入的数据（非RET前缀）同步到主数据表
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
                
    def beldi_commit(self, transaction_id):
        self.sync_shadow_to_data_db_with_version(transaction_id)

    # def release_lock(self, transaction_id, lock_set):
    #     """
    #     释放 lock_set 中每个 key 的锁，将其 lock 属性设置为 None。时间戳也置为None
    #     """

    #     for key in lock_set.keys():
    #         # 更新 lock 属性为 None
    #         lock_item = self.data_db.get_item(
    #             Key={'key': key}
    #         )
    #         try:
    #             self.data_db.update_item(
    #                 Key={'key': key},
    #                 UpdateExpression="SET #l = :none, #ct = :none",
    #                 ExpressionAttributeNames={
    #                     '#l': 'lock',
    #                     '#ct': 'create_timestamp'
    #                 },
    #                 ConditionExpression="#l = :txid",  # 确保当前锁属于 transaction_id
    #                 ExpressionAttributeValues={
    #                     ':txid': transaction_id,
    #                     ':none': None
    #                 },
    #                 ReturnValues="UPDATED_NEW"
    #             )
    #         except:
    #             logging.info(f"the lock has been released by another branch, skip.")
    #             continue
            
        
