import logging
import time
from botocore.exceptions import ClientError
from decimal import Decimal

class PassiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "ACTIVE"
        self.message = message

class PassiveAbortException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.abort_type = "PASSIVE"
        self.message = message

class BeldiStore:
    def __init__(self, db_server):
        self.db_server = db_server
        self.data_db = db_server.Table('data')

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
            logging.error(f"CRITICAL: Failed to roll back global lock for key '{key}'. Manual cleanup may be required. Error: {e}")

    def acquire_lock(self, key):
        start_time = time.time()
        
        # 步骤 1: 检查是否已持有锁 (快速路径)
        if self.lock_shadow_table.get_item(Key={'key': key}).get('Item'):
            return time.time() - start_time

        # # 步骤 2: 验证 Term 是否有效
        # try:
        #     term_item = self.lock_shadow_table.get_item(Key={'key': '_term_'}).get('Item')
        #     # 统一使用 'value' 作为存储 term 的属性
        #     if not term_item or int(term_item.get('value', -1)) != self.term:
        #         raise PassiveAbortException(f"Term mismatch. My term: {self.term}, table term: {term_item.get('value', 'N/A')}. Aborting.")
        # except ClientError:
        #      raise PassiveAbortException(f"Failed to read term from lock shadow table for tx {self.transaction_id}. Aborting.")

        # 步骤 3: 循环尝试获取全局锁
        max_wait_time = 6
        lock_timeout = 5
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
                    item = response.get('Item')
                    if not item: continue
                    locker_txid = item.get('lock')
                    current_lock_timestamp = item.get('create_timestamp')
                    if not locker_txid or current_lock_timestamp is None: 
                        raise Exception("Unexpected lock state: lock exists but missing details.")
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
            self.db_server.transact_write_items(
                TransactItems=[
                    {
                        'ConditionCheck': {
                            'TableName': self.lock_shadow_table.name,
                            'Key': {'key': {'S': '_term_'}},
                            'ConditionExpression': '#v = :term',
                            'ExpressionAttributeNames': {'#v': 'value'},
                            'ExpressionAttributeValues': {':term': {'N': str(self.term)}}
                        }
                    },
                    {
                        'Put': {
                            'TableName': self.lock_shadow_table.name,
                            'Item': {'key': {'S': key}, 'value': {'N': '1'}}
                        }
                    }
                ]
            )
        except ClientError as e:
            # 检查事务是否因为条件检查失败而取消
            if e.response['Error']['Code'] == 'TransactionCanceledException' and \
               e.response['CancellationReasons'][0]['Code'] == 'ConditionalCheckFailed':
                self._release_global_lock(key)
                raise PassiveAbortException(f"Term changed while trying to record lock for key '{key}'. Aborting.")
            else:
                self._release_global_lock(key)
                raise PassiveAbortException(f"DynamoDB transaction error while recording lock for key '{key}': {e}")

        return time.time() - start_time


    # def _check_and_handle_expired_lock(self, key, lock_timeout):
    #     """检查并处理过期的锁。"""
    #     response = self.data_db.get_item(Key={'key': key}, ConsistentRead=True)
    #     item = response.get('Item')
    #     if not item:
    #         return # 锁已被释放

    #     locker_txid = item.get('lock')
    #     lock_timestamp = item.get('create_timestamp')

    #     if not locker_txid or lock_timestamp is None:
    #         return # 锁信息不完整

    #     if time.time() - float(lock_timestamp) > lock_timeout:
    #         self.data_db.update_item(
    #             Key={'key': key},
    #             UpdateExpression="SET #l = :none, #ct = :none",
    #             ConditionExpression="#l = :locker AND #ct = :old_time",
    #             ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
    #             ExpressionAttributeValues={':none': None, ':locker': locker_txid, ':old_time': lock_timestamp}
    #         )