import logging
import time
from botocore.exceptions import ClientError
from decimal import Decimal

class ActiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "ACTIVE"
        self.message = message

class PassiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "PASSIVE"
        self.message = message

class ERRORAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "ERROR"
        self.message = message

class BeldiStore:
    def __init__(self, db_server):
        self.db_server = db_server
        self.data_db = db_server.Table('data')
        self.dynamo_client = db_server.meta.client

    def runtime_init(self, transaction_id, create_timestamp, term):
        self.transaction_id = transaction_id
        self.create_timestamp = Decimal(str(create_timestamp))
        self.term = term
        self.shadow_table = self.db_server.Table(f"{self.transaction_id}_shadow_table")
        self.lock_shadow_table = self.db_server.Table(f"{self.transaction_id}_lock_shadow_table")


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
            ## logging.info(f"RYW: {key}, upstream_func: {upstream_func}")
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
        
    def _release_global_lock(self, key):
        try:
            self.data_db.update_item(
                Key={'key': key},
                UpdateExpression="SET #l = :none, #ct = :none",
                ExpressionAttributeNames={
                    '#l': 'lock',
                    '#ct': 'create_timestamp'
                },
                ConditionExpression="#l = :txid",  # 确保当前锁属于 transaction_id
                ExpressionAttributeValues={
                    ':txid': self.transaction_id,
                    ':none': None
                },
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as e:
            print(f"CRITICAL: Failed to roll back global lock for key '{key}'. Manual cleanup may be required. Error: {e}", flush=True)

    def acquire_lock(self, key):
        start_time = time.time()
        
        # 步骤 1: 检查是否已持有锁 (快速路径)
        if self.lock_shadow_table.get_item(Key={'key': key}).get('Item'):
            #print(f"Lock for key '{key}' already exists in shadow table, skipping global lock acquisition.", flush=True)
            return time.time() - start_time

        #print(f"Acquiring global lock for key: {key}, key type:{type(key)} transaction_id: {self.transaction_id}")
        # # 步骤 2: 验证 Term 是否有效
        try:
            term_item = self.lock_shadow_table.get_item(Key={'key': '_term_'}).get('Item')
            # 统一使用 'value' 作为存储 term 的属性
            if not term_item or term_item.get('value', -1) != self.term:
                raise PassiveAbortException(f"Term mismatch. My term: {self.term}, table term: {term_item.get('value', 'N/A')}. Aborting.")
        except ClientError:
             raise PassiveAbortException(f"Failed to read term from lock shadow table for tx {self.transaction_id}. Aborting.")

        # 步骤 3: 循环尝试获取全局锁
        max_wait_time = 6
        lock_timeout = 10
        while time.time() - start_time < max_wait_time:
            try:
                self.data_db.update_item(
                    Key={'key': key},
                    UpdateExpression="SET #l = :txid, #ct = :time",
                    ConditionExpression="attribute_not_exists(#l) OR #l = :none",
                    ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
                    ExpressionAttributeValues={':txid': self.transaction_id, ':none': None, ':time': self.create_timestamp}
                )
                break
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    response = self.data_db.get_item(Key={'key': key}, ConsistentRead=True)
                    self._check_and_handle_expired_lock(key, lock_timeout)
                    item = response.get('Item')
                    
                    if not item: continue
                    locker_txid = item.get('lock')
                    current_lock_timestamp = item.get('create_timestamp')
                    if not locker_txid: 
                        time.sleep(0.005)
                        continue

                    if locker_txid == self.transaction_id:
                        # 这是一个不应该发生的状态，意味着快速路径检查和全局锁状态不一致。
                        # 可能是由于之前的操作未能完全清理。直接中止以避免死锁。
                        raise ERRORAbortException(f"CRITICAL: Detected own lock on key '{key}' in global table but not in shadow table. Aborting.")

                    if self.create_timestamp > current_lock_timestamp:
                        raise PassiveAbortException(f"Lock acquisition failed for key {key}: older transaction {locker_txid} holds the lock.")
                    else:
                        time.sleep(0.005)
                        continue
                else:
                    raise PassiveAbortException(f"DynamoDB error while acquiring global lock for key {key}: {e}")
        else:
            raise PassiveAbortException(f"Lock acquisition timeout for key {key} after {max_wait_time}s")

        # 步骤 4: 在锁影表中原子地记录已获取的锁
        try:
            self.lock_shadow_table.update_item(
                Key={'key': '_term_'},
                # 我们并不真的想更新 _term_ 项，只是想借用它的条件检查功能。
                # 所以这里做一个无意义的更新。
                UpdateExpression="SET #last_lock_ts = :ts",
                ConditionExpression="#v = :current_term",
                ExpressionAttributeNames={
                    '#v': 'value',
                    '#last_lock_ts': 'last_lock_timestamp'
                },
                ExpressionAttributeValues={
                    ':current_term': self.term,
                    ':ts': self.create_timestamp
                }
            )
            # 条件检查通过后，再安全地写入真正的锁记录
            self.lock_shadow_table.put_item(Item={'key': key, 'value': 1})

        except ClientError as e:
            # 检查事务是否因为条件检查失败而取消
            if e.response['Error']['Code'] == 'TransactionCanceledException' and \
               e.response['CancellationReasons'][0]['Code'] == 'ConditionalCheckFailed':
                #print(f"Term changed while trying to record lock for key '{key}'. Rolling back global lock.", flush=True)
                self._release_global_lock(key)
                raise PassiveAbortException(f"Term changed while trying to record lock for key '{key}'. Aborting.")
            else:
                #print(f"DynamoDB transaction error while recording lock for key '{key}': {e}", flush=True)
                raise Exception(f"DynamoDB transaction error while recording lock for key '{key}': {e}")

        return time.time() - start_time


    def _check_and_handle_expired_lock(self, key, lock_timeout):
        """检查并处理过期的锁。"""
        response = self.data_db.get_item(Key={'key': key}, ConsistentRead=True)
        item = response.get('Item')
        if not item:
            return # 锁已被释放

        locker_txid = item.get('lock')
        lock_timestamp = item.get('create_timestamp')

        if not locker_txid or lock_timestamp is None:
            return # 锁信息不完整

        if time.time() - float(lock_timestamp) > lock_timeout:
            self.data_db.update_item(
                Key={'key': key},
                UpdateExpression="SET #l = :none, #ct = :none",
                ConditionExpression="#l = :locker AND #ct = :old_time",
                ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
                ExpressionAttributeValues={':none': None, ':locker': locker_txid, ':old_time': lock_timestamp}
            )