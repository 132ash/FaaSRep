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
        self.shadow_table = self.db_server.Table("shadow_table")
        self.lock_shadow_table = self.db_server.Table("lock_shadow_table")
        self.dynamo_client = db_server.meta.client

    def runtime_init(self, transaction_id, create_timestamp, term):
        self.transaction_id = transaction_id
        self.create_timestamp = Decimal(str(create_timestamp))
        self.term = term
        self.lock_latency = 0
        self.io_latency = 0

    def put(self, key, value, this_func="", upstream_func="", write_set={}, ret=False):
        lock_time = 0
        # have upstream func: no need for lock. change the write func.
        if not upstream_func:
            # put 操作需要获取写锁 ('W')
            lock_time = self.acquire_lock(key, 'W')
        start=time.time()
        self.shadow_table.put_item(
            Item={
                'txid': self.transaction_id, # 分区键
                'key': key,                  # 排序键
                'value': str(value)
            }
        )
        self.io_latency += time.time() - start
        if not ret:
            write_set[key] = this_func
        return lock_time    

    def get(self, key, upstream_func, log=True):
        item = None
        value = None
        lock_time = 0
        # RYW. not acquire lock, read from shadow table.
        if upstream_func:
            ## logging.info(f"RYW: {key}, upstream_func: {upstream_func}")
            start = time.time()
            response = self.shadow_table.get_item(
                    Key={
                        'txid': self.transaction_id, # 分区键
                        'key': key                   # 排序键
                    }
                )
            item = response.get('Item')
            if log:
                self.io_latency += time.time() - start
        else:
            # get 操作需要获取读锁 ('R')
            lock_time = self.acquire_lock(key, 'R')
            start = time.time()
            response = self.data_db.get_item(
                Key={
                    'key': key
                }
            )
            item = response.get('Item')
            self.io_latency += time.time() - start
        value = item['value'] if item else None
        if item:
            return value, lock_time
        else:
            return "", 0
        
    def _release_global_lock(self, key):
        try:
            # 释放锁时，只需检查 txid，因为只有自己持有的锁才需要自己释放
            self.data_db.update_item(
                Key={'key': key},
                UpdateExpression="REMOVE #l, #ct",
                ConditionExpression="#l.txid = :txid",
                ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
                ExpressionAttributeValues={':txid': self.transaction_id}
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                print(f"CRITICAL: Failed to roll back global lock for key '{key}'. Manual cleanup may be required. Error: {e}", flush=True)

    def acquire_lock(self, key, lock_type):
        start_time = time.time()
        
        # 步骤 1: 快速路径检查
        if self.lock_shadow_table.get_item(Key={'txid': self.transaction_id, 'key': key}).get('Item'):
            self.lock_latency += time.time() - start_time
            return time.time() - start_time

        # 步骤 2: 验证 Term
        try:
            term_item = self.lock_shadow_table.get_item(Key={'txid': self.transaction_id, 'key': '_term_'}).get('Item')
            if not term_item or term_item.get('value', -1) != self.term:
                self.lock_latency += time.time() - start_time
                raise PassiveAbortException(f"Term mismatch. My term: {self.term}, table term: {term_item.get('value', 'N/A')}. Aborting.")
        except ClientError:
             self.lock_latency += time.time() - start_time
             raise PassiveAbortException(f"Failed to read term from lock shadow table for tx {self.transaction_id}. Aborting.")

        # 步骤 3: 循环尝试获取全局锁
        max_wait_time = 6
        lock_acquired = False
        while time.time() - start_time < max_wait_time:
            # --- 尝试加锁 ---
            try:
                # 读锁逻辑
                if lock_type == 'R':
                    # 检查当前锁状态，如果已经是读锁，直接成功
                    current_lock = self.data_db.get_item(Key={'key': key}, ConsistentRead=True).get('Item', {}).get('lock')
                    if current_lock and current_lock.get('type') == 'R':
                        self.lock_latency += time.time() - start_time
                        return time.time() - start_time # 共享读锁，直接成功，不记录到 shadow table

                    # 尝试加读锁 (仅当无锁时)
                    self.data_db.update_item(
                        Key={'key': key},
                        UpdateExpression="SET #l = :lock_info, #ct = :time",
                        ConditionExpression="attribute_not_exists(#l) OR #l = :none",
                        ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
                        ExpressionAttributeValues={
                            ':lock_info': {'type': 'R', 'txid': self.transaction_id},
                            ':time': self.create_timestamp,
                            ':none': None
                        }
                    )
                # 写锁逻辑
                else: # lock_type == 'W'
                    self.data_db.update_item(
                        Key={'key': key},
                        UpdateExpression="SET #l = :lock_info, #ct = :time",
                        ConditionExpression="attribute_not_exists(#l) OR #l = :none",
                        ExpressionAttributeNames={'#l': 'lock', '#ct': 'create_timestamp'},
                        ExpressionAttributeValues={
                            ':lock_info': {'type': 'W', 'txid': self.transaction_id},
                            ':time': self.create_timestamp,
                            ':none': None
                        }
                    )
                break # 成功获取到新锁，跳出循环

            except ClientError as e:
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    self.lock_latency += time.time() - start_time
                    raise PassiveAbortException(f"DynamoDB error while acquiring lock for key {key}: {e}")
                
                response = self.data_db.get_item(Key={'key': key}, ConsistentRead=True)
                item = response.get('Item')
                
                if not item: continue # 锁被释放了，下一轮循环会尝试加锁
                
                lock_info = item.get('lock')
                current_lock_timestamp = item.get('create_timestamp')

                if not lock_info or not current_lock_timestamp:
                    time.sleep(0.005)
                    continue

                locker_txid = lock_info.get('txid')
                if locker_txid == self.transaction_id:
                    self.lock_latency += time.time() - start_time
                    raise ERRORAbortException(f"CRITICAL: Detected own lock on key '{key}' in global table but not in shadow table.")

                if self.create_timestamp > current_lock_timestamp:
                    self.lock_latency += time.time() - start_time
                    raise PassiveAbortException(f"Lock acquisition failed for key {key}: older transaction {locker_txid} holds the lock.")
                else:
                    time.sleep(0.005)
                    continue

        # 步骤 4: 在锁影表中记录已获取的锁 (只有成功获取新锁时才执行)
        try:
            self.lock_shadow_table.update_item(
                Key={'txid': self.transaction_id, 'key': '_term_'},
                UpdateExpression="SET #last_lock_ts = :ts",
                ConditionExpression="#v = :current_term",
                ExpressionAttributeNames={'#v': 'value', '#last_lock_ts': 'last_lock_timestamp'},
                ExpressionAttributeValues={':current_term': self.term, ':ts': self.create_timestamp}
            )
            # 保持 shadow table 结构不变
            self.lock_shadow_table.put_item(Item={'txid': self.transaction_id, 'key': key, 'value': 1})
        except ClientError as e:
            self.lock_latency += time.time() - start_time
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self._release_global_lock(key)
                raise PassiveAbortException(f"Failed to record lock for key '{key}': term mismatch or transaction is committing.")
            else:
                self._release_global_lock(key)
                raise Exception(f"DynamoDB error while recording lock for key '{key}': {e}")
        
        self.lock_latency += time.time() - start_time
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