import logging
import time
from botocore.exceptions import ClientError
from decimal import Decimal

class BeldiStore:
    def __init__(self, db_server):
        self.db_server = db_server
        self.data_db = db_server.Table('data')

    def runtime_init(self, transaction_id, lock_set, create_timestamp):
        self.transaction_id = transaction_id
        self.lock_set = lock_set
        self.create_timestamp = Decimal(str(create_timestamp))
        self.shadow_table = self.db_server.Table(f"{self.transaction_id}_shadow_table")


    def put(self, key, value, this_func="", upstream_func="", write_set={}, ret=False):
        lock_time = 0
        # have upstream func: no need for lock. change the write func.
        if not upstream_func:
            lock_time = self.acquire_lock(key)
        self.shadow_table.put_item(
            Item={
                'key': key,
                'value': str(value)
            }
        )
        if not ret:
            write_set[key] = this_func
        return lock_time    

    def get(self, key, upstream_func):
        item = None
        value = None
        lock_time = 0
        # RYW. not acquire lock, read from shadow table.
        if upstream_func:
            #logging.info(f"RYW: {key}, upstream_func: {upstream_func}")
            response = self.shadow_table.get_item(
                    Key={
                        'key': key
                    }
                )
            item = response.get('Item')
        else:
            lock_time = self.acquire_lock(key)
            response = self.data_db.get_item(
                Key={
                    'key': key
                }
            )
            item = response.get('Item')
        value = item['value'] if item else None
        if item:
            return value, lock_time
        else:
            return "", 0

    def acquire_lock(self, key):        
        start = time.time()
        if self.lock_set.get(key, False):
            return time.time() - start
        else:
            while True:
                try:
                    # 尝试获取锁
                    self.data_db.update_item(
                        Key={'key': key},
                        UpdateExpression="SET #l = :txid, #ct = :create_timestamp",
                        ConditionExpression="attribute_not_exists(#l) OR #l = :none OR #l = :txid",
                        ExpressionAttributeNames={
                            '#l': 'lock',
                            '#ct': 'create_timestamp'
                        },
                        ExpressionAttributeValues={
                            ':txid': self.transaction_id,
                            ':none': None,
                            ':create_timestamp': self.create_timestamp
                        },
                        ReturnValues="UPDATED_NEW"
                    )
                    # 获取锁成功
                    self.lock_set[key] = True
                    return time.time() - start
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                        # 获取锁失败，检查当前锁持有者的时间戳
                        response = self.data_db.get_item(
                            Key={'key': key}
                        )
                        item = response.get('Item')
                        locker_txid = item['lock']
                        current_lock_timestamp = item['create_timestamp']
                        if current_lock_timestamp == None:
                            raise Exception(f"current_lock_timestamp is None. Key: {key},locker_txid: {locker_txid}")
                        elif self.create_timestamp < current_lock_timestamp:
                            # 自己的时间戳更早，继续尝试
                            logging.info(f"Transaction {self.transaction_id} waiting for lock on key {key} (earlier timestamp)")
                            time.sleep(0.005)  # 短暂等待后重试
                            continue
                        else:
                            # 自己的时间戳较晚，抛出异常
                            logging.error(f"Transaction {self.transaction_id} aborted due to later timestamp on key {key}")
                            raise Exception(f"Lock acquisition failed for key {key}: newer than holder {locker_txid}.")
            