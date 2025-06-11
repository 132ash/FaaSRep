import logging
import time

class BeldiStore:
    def __init__(self, db_server):
        self.db_server = db_server
        self.data_db = db_server.Table('data')

    def runtime_init(self, transaction_id, lock_set):
        self.transaction_id = transaction_id
        self.lock_set = lock_set
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
            logging.info(f"RYW: {key}, upstream_func: {upstream_func}")
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
            self.data_db.update_item(
                Key={'key': key},
                UpdateExpression="SET #l = :txid",
                ConditionExpression="attribute_not_exists(#l) OR #l = :none OR #l = :txid",
                ExpressionAttributeNames={
                    '#l': 'lock'
                },
                ExpressionAttributeValues={
                    ':txid': self.transaction_id,
                    ':none': None
                },
                ReturnValues="UPDATED_NEW"
            )
            response = self.data_db.get_item(
                Key={
                    'key': key
                }
            )
            item = response.get('Item')
            end = time.time()
            logging.info(f"acquire lock for {key}, lock:{item['lock']}")
            self.lock_set[key] = True
            return end - start      
       