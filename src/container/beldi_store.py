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

    def acquire_lock(self, key):        
        start = time.time()
        if self.lock_set.get(key, False):
            return time.time() - start
        else:
            max_wait_time = 6  # 最大等待6秒
            lock_timeout = 5  # 锁的超时时间5s
            
            while time.time() - start < max_wait_time:
                try:
                    # 尝试获取锁
                    self.data_db.update_item(
                        Key={'key': key},
                        UpdateExpression="SET #l = :txid, #ct = :time",
                        ConditionExpression="attribute_not_exists(#l) OR #l = :none OR #l = :txid",
                        ExpressionAttributeNames={
                            '#l': 'lock',
                            '#ct': 'create_timestamp'
                        },
                        ExpressionAttributeValues={
                            ':txid': self.transaction_id,
                            ':none': None,
                            ':time': self.create_timestamp
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
                            Key={'key': key},
                            ConsistentRead=True
                        )
                        item = response.get('Item')
                        if not item:
                            time.sleep(0.005)
                            continue
                            
                        locker_txid = item.get('lock')
                        current_lock_timestamp = item.get('create_timestamp')
                        
                        if not locker_txid:
                            time.sleep(0.005)
                            continue
                            
                        if current_lock_timestamp is None:
                            raise Exception(f"Lock exists but no timestamp. Key: {key}, locker_txid: {locker_txid}")
                        
                        # 检查锁是否已经超时
                        current_time = time.time()
                        lock_age = current_time - float(current_lock_timestamp)
                        
                        if lock_age > lock_timeout:
                            # 锁已超时，尝试清理
                            logging.warning(f"Detected expired lock for key {key}, age: {lock_age}s, attempting cleanup")
                            try:
                                self.data_db.update_item(
                                    Key={'key': key},
                                    UpdateExpression="SET #l = :none, #ct = :none",
                                    ConditionExpression="#l = :locker AND #ct = :old_time",
                                    ExpressionAttributeNames={
                                        '#l': 'lock',
                                        '#ct': 'create_timestamp'
                                    },
                                    ExpressionAttributeValues={
                                        ':none': None,
                                        ':locker': locker_txid,
                                        ':old_time': current_lock_timestamp
                                    }
                                )
                                logging.info(f"Successfully cleaned expired lock for key {key}")
                                continue  # 重试获取锁
                            except ClientError:
                                # 清理失败，可能锁已被其他事务更新
                                logging.warning(f"Failed to clean expired lock for key {key}")
                        
                        if self.create_timestamp < current_lock_timestamp:
                            time.sleep(0.005)
                            continue
                        else:
                            raise PassiveAbortException(f"Lock acquisition failed for key {key}: newer than holder {locker_txid}.")
                    else:
                        raise Exception(f"Error acquiring lock on key {key}: {e}")
            
            # 超时后仍未获取到锁
            raise PassiveAbortException(f"Lock acquisition timeout for key {key} after {max_wait_time}s")