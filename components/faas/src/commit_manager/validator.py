import sys
from TX_timestamp import TimeStampAllocator
import gevent
import gevent.lock
import threading
from typing import Any, Dict, List
from TX_timestamp import TxVersion
import components.faas.src.commit_manager.TX_timestamp as TX_timestamp
import requests
import validator_repo

sys.path.append('../../config')
import config
repo = validator_repo.Repository()

class TxValidator:

    def __init__(self, timestamp_allocator:TimeStampAllocator):
        self.global_table = repo.get_initial_data_version() # key:version table
        self.locks = {}  # wait infomation and lock for each key 
        self.acquired_locks = {} # acquired locks for each transaction
        self.lock_key_set_per_tx = {} # lock key set for each transaction
        self.write_set_per_tx = {} # write set for each transaction
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
    
    def validate(self, transaction_id, read_set: Dict[str, Dict], write_set: Dict[str, int], function_pos) -> tuple[dict, bool]:
        expired_keys:Dict[str:List] = {}
        lock_key_set = set()
        self.acquired_locks[transaction_id] = {"target":0, "acquired":0, "cond":threading.Condition()}

        self.timestamp_allocator.wait_for_preceeding_txs(transaction_id)

        # collect all keys in write set.
        self.write_set_per_tx[transaction_id] = write_set
        for key in write_set.keys():
            lock_key_set.add(key)
            self.ask_for_lock(transaction_id, key)

        # collect all keys in read set.
        for func, rs in read_set.items():
            expired_keys[func] = []
            for key, version in rs.items():
                lock_key_set.add(key)
                self.ask_for_lock(transaction_id, key)

        # FINISHED ask for locks, notify the next tx.
        self.timestamp_allocator.notify_next_tx(transaction_id)
        

        self.acquired_locks[transaction_id]["target"] = len(lock_key_set)
        while self.acquired_locks[transaction_id]["acquired"] < self.acquired_locks[transaction_id]["target"]:
            with self.acquired_locks[transaction_id]["cond"]:
                self.acquired_locks[transaction_id]["cond"].wait()

        for func, rs in read_set.items():
            expired_keys[func] = {}
            for key, version in rs.items():
                if version < self.global_table.get(key):
                    ip = function_pos[func]
                    if ip not in expired_keys:
                        expired_keys[ip] = []
                    expired_keys[ip].append(key)

        self.lock_key_set_per_tx[transaction_id] = lock_key_set

        return expired_keys, len(expired_keys) != 0
    
    # modify global table, and release locks.
    def commit_tx(self, transaction_id, version: TxVersion):
        write_set = self.write_set_per_tx[transaction_id]
        all_addr_ip = set()

        for key, func_ip_pair in write_set.items():
            all_addr_ip.add(func_ip_pair['ip'])
            self.global_table[key] = version.to_string()

        jobs = [
            gevent.spawn(self.trigger_worker_commit, ip, transaction_id, version)
            for ip in all_addr_ip
        ]
        gevent.joinall(jobs)
        
        for key in self.lock_key_set_per_tx[transaction_id]:
            self.release_lock(transaction_id, key)
        self.acquired_locks.pop(transaction_id)
        self.write_set_per_tx.pop(transaction_id)
        self.lock_key_set_per_tx.pop(transaction_id)

    def trigger_worker_commit(self, ip, transaction_id, version):
        url = f"http://{ip}/commit"
        
        data = {
            'transaction_id': transaction_id,
            "version": version
        }
        requests.post(url, json=data)
