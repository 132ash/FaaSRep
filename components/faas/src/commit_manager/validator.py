import sys
from TX_timestamp import TimeStampAllocator
import gevent
import gevent.lock
import threading
from typing import Any, Dict, List
import requests
import components.faas.src.commit_manager.TX_timestamp as TX_timestamp
from datetime import datetime

sys.path.append('../../config')
import config

class TxVersion:
    def __init__(self, transaction_id: str, commit_timestamp: str):
        self.transaction_id = transaction_id
        self.commit_timestamp = commit_timestamp

    def __lt__(self, other):
        return self.commit_timestamp < other.commit_timestamp

    def __le__(self, other):
        return self.commit_timestamp <= other.commit_timestamp

    def __eq__(self, other):
        return self.commit_timestamp == other.commit_timestamp

    def __ne__(self, other):
        return self.commit_timestamp != other.commit_timestamp

    def __gt__(self, other):
        return self.commit_timestamp > other.commit_timestamp

    def __ge__(self, other):
        return self.commit_timestamp >= other.commit_timestamp

    def to_string(self) -> str:
        # 将 TxVersion 对象转换为紧凑的字符串
        return f"{self.transaction_id}:{self.commit_timestamp}"

    @classmethod
    def from_string(cls, version_str: str):
        # 从符合格式的字符串初始化一个 TxVersion 对象
        transaction_id, commit_timestamp = version_str.split(':')
        return cls(transaction_id, commit_timestamp)

class TxValidator:

    def __init__(self, timestamp_allocator:TimeStampAllocator):
        self.global_table = {}  # key:version table
        self.locks = {}  # wait infomation and lock for each key 
        self.acquired_locks = {} # acquired locks for each transaction
        self.timestamp_allocator = timestamp_allocator
    
    def get_lock(self, key):
        if key not in self.locks:
            self.locks[key] = {'waiters': [], 'waiter_lock': threading.Lock()}
        return self.locks[key]
    
    # append tx_id to waiters list and wait for the lock.

    def ask_for_lock(self, tx_id, key):
        lock = self.get_lock(key)
        lock['waiters'].append(tx_id)
        if lock['waiters'][0] == tx_id:
            self.acquired_locks[tx_id]["acquired"] += 1
        

    def release_lock(self, tx_id, key):
        try:
            if self.locks[key]["waiter"][0] == tx_id:
                self.locks[key]["waiters"].pop(0)
                if len(self.locks[key]["waiters"]) > 0:
                    next_tx_id = self.locks[key]["waiters"][0]
                    with self.acquired_locks[next_tx_id]["cond"]:
                        self.acquired_locks[next_tx_id]["cond"].notify_all()
        except:
            raise Exception("release lock failed, tx_id not the owner of the lock")
    
    def validate(self, transaction_id, read_set: Dict[str, Dict], write_set: Dict[str, int]) -> tuple[dict, bool]:
        expired_keys = {}
        lock_key_set = set()
        self.acquired_locks[transaction_id] = {"target":0, "acquired":0, "cond":threading.Condition()}

        self.timestamp_allocator.wait_for_preceeding_txs(transaction_id)

        # collect all keys in write set.
        for key in write_set:
            lock_key_set.add(key)

        # collect all keys in read set.
        for func, rs in read_set.items():
            expired_keys[func] = {}
            for key, version in rs.items():
                lock_key_set.add(key)
        
        # wait for all locks to be acquired.
        self.acquired_locks[transaction_id]["target"] = len(lock_key_set)
        for key in lock_key_set:
            self.ask_for_lock(transaction_id, key)
        # FINISHED ask for locks, notify the next tx.
        self.timestamp_allocator.notify_next_tx(transaction_id)
        with self.acquired_locks[transaction_id]["cond"]:
            while self.acquired_locks[transaction_id]["acquired"] < self.acquired_locks[transaction_id]["target"]:
                self.acquired_locks[transaction_id]["cond"].wait()

        for func, rs in read_set.items():
            expired_keys[func] = {}
            for key, version in rs.items():
                if version < self.global_table.get(key):
                    expired_keys[func][key] = version

        return lock_key_set, expired_keys, len(expired_keys) != 0

    def update_global_table(self, write_set: Dict[str, int], version):
        for key, version in write_set.items():
            self.global_table[key] = version

