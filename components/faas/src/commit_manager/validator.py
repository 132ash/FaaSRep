import sys
from gevent import monkey
monkey.patch_all()
from TX_timestamp import TimeStampAllocator
import gevent
import gevent.lock
import logging
from typing import Any, Dict, List
from TX_timestamp import BatchVersion
from gevent import event
import requests
import validator_repo
import validate_metadata

sys.path.append('../../config')
import config
repo = validator_repo.Repository()

class BatchValidator:

    def __init__(self, timestamp_allocator:TimeStampAllocator):
        self.global_table = repo.get_initial_data_version() # key:version table
        self.locks = {}  # wait infomation and lock for each key 
        self.acquired_locks = {} # acquired locks for each batch
        self.lock_key_set_per_batch = {} # lock key set for each batch: {batch_id: [keys]}, used for releasing
        self.write_set_per_batch = {} # write set for each transaction in a batch: {batch_id: {key:set(), ip:set()}}, used for updating versions
        self.tx_list_per_batch = {} # [tx1, tx2, ... ]
        self.damaged_functions_per_batch = {} # dirty functions

        self.expired_keys_per_batch = {} # for updating caches
        self.subjection_table = validate_metadata.SubjectionTable()
        self.timestamp_allocator = timestamp_allocator

    def get_lock(self, key):
        if key not in self.locks:
            self.locks[key] = {'waiters': [], "writer":{"tx_id":None, "batch_id":None, "name":None}, "informed":False}
        return self.locks[key]
    
    # append tx_id to waiters list and wait for the lock.
    # for key write: cover the last written sign.
    # for key read: check if the same key is written before by a previous tx in the same batch.
    def ask_for_lock(self, batch_id, tx_id, func, key, mode):
        already_waiting = True
        prev_batch_id = None
        upstream_tx_id = None
        upstream_writer = None
        lock = self.get_lock(key)
        if len(lock["waiters"]) == 0 or lock["waiters"][-1] != batch_id:
            already_waiting = False
            lock['waiters'].append(batch_id)
        if mode == "write":
            lock["writer"]["tx_id"] = tx_id
            lock["writer"]["batch_id"] = batch_id
            lock["writer"]["name"] = func
        if mode == "read":
            prev_batch_id = lock["writer"]["batch_id"]
            upstream_tx_id = lock["writer"]["tx_id"]
            upstream_writer = lock["writer"]["name"]
        # if the tx is the first waiter, then check if it is informed (have the lock).
        # if not, then acquire the lock.
        if lock['waiters'][0] == batch_id:
            if not lock["informed"]:
                self.acquired_locks[batch_id]["acquired"] += 1
            else:
                lock["informed"] = False
        return already_waiting, prev_batch_id, upstream_tx_id, upstream_writer

    def release_lock(self, batch_id, key):
        if self.locks[key]["waiters"][0] == batch_id:
            self.locks[key]["waiters"].pop(0)
            if len(self.locks[key]["waiters"]) > 0:
                next_batch_id = self.locks[key]["waiters"][0]
                self.acquired_locks[next_batch_id]["acquired"] += 1
                self.acquired_locks[next_batch_id]["cond"].set()
                # inform next waiter.
                self.locks[key]["informed"] = True
        else:
            raise Exception("release lock failed, tx_id not the owner of the lock")

    def get_subjection_table_for_batch(self, batch_id):
        return self.subjection_table.get_table_for_batch(batch_id)
    
    def target_expired_keys_per_node(self, batch_id, ip, key):
        expired_keys_for_ip = self.expired_keys_per_batch[batch_id].get(ip, set())
        expired_keys_for_ip.add(key)
        self.expired_keys_per_batch[batch_id][ip] = expired_keys_for_ip

    def target_damaged_funcs_per_tx(self, batch_id, tx_id, func):
        damaged_functions_for_tx = self.damaged_functions_per_batch[batch_id].get(tx_id, {})
        damaged_functions_for_tx[func]=True
        self.damaged_functions_per_batch[batch_id][tx_id] = damaged_functions_for_tx    


    def validate(self, batch_id, workflow_name_per_tx, read_set_per_batch, write_set_per_batch, transaction_list, function_pos_per_tx):
        self.acquired_locks[batch_id] = {"target":0, "acquired":0, "cond":event.Event()}
        self.acquired_locks[batch_id]["cond"].clear()
        self.lock_key_set_per_batch[batch_id] = []
        self.write_set_per_batch[batch_id] = write_set_per_batch
        self.tx_list_per_batch[batch_id] = transaction_list
        self.expired_keys_per_batch[batch_id] = {}
        self.damaged_functions_per_batch[batch_id] = {}

        expired_keys:Dict[str:set] = {}

        # wait for the previous batch to finish asking locks.
        self.timestamp_allocator.wait_for_preceeding_batches(batch_id)

        for tx_id in transaction_list:
            write_set = write_set_per_batch[tx_id]
            read_set = read_set_per_batch[tx_id]
            workflow_name = workflow_name_per_tx[tx_id]
            self.subjection_table.init(batch_id)

            # keys in read set: 
            # check if the same key is written before by a previous tx in the same batch. then fillup subjection table.
            for func, kv_pairs in read_set.items():
                for key in kv_pairs.keys():
                    already_waiting, prev_batch_id, upstream_tx_id, upstream_writer = self.ask_for_lock(batch_id, tx_id , func, key, "read")
                    # calculate total number of locks to be acquired.
                    if not already_waiting:
                        self.acquired_locks[batch_id]["target"] += 1
                        self.lock_key_set_per_batch[batch_id].append(key)
                    #subjection across txs: read a key that is written by a previous tx in the same batch.
                    if batch_id == prev_batch_id and upstream_tx_id != tx_id:
                        # the last writer is not commited yet, damaged for sure.
                        self.target_damaged_funcs_per_tx(batch_id, tx_id, func)
                        self.subjection_table.update_subjection_table(batch_id, workflow_name, upstream_tx_id, tx_id, upstream_writer, func, function_pos_per_tx[upstream_tx_id][upstream_writer], ip, key)

            # keys in write set:
            # cover the last written sign.
            for key, write_key_info in write_set.items():
                func = write_key_info['func']
                ip = write_key_info['ip']
                already_waiting, _, _, _ = self.ask_for_lock(batch_id, tx_id , func, key, "write")
                if not already_waiting:
                    self.acquired_locks[batch_id]["target"] += 1
                    self.lock_key_set_per_batch[batch_id].append(key)

            logging.info(f"transaction {tx_id} finished asking for locks.")

        # FINISHED ask for locks, notify the next tx.
        self.timestamp_allocator.notify_next_batch(batch_id)
        
        # waiting for all locks in the batch to be acquired.
        while self.acquired_locks[batch_id]["acquired"] < self.acquired_locks[batch_id]["target"]:
            self.acquired_locks[batch_id]["cond"].wait()
            self.acquired_locks[batch_id]["cond"].clear()

        # check the keys in read set, if the version is expired.
        # collect expired keys for each node and damaged functions.
        expired_keys:Dict[str:list] = {}
        damaged_funcs:Dict[str:set] = {}
        for txid, rs in read_set_per_batch.items():
            for func, kv_pair in rs.items():
                    for key, version in kv_pair.items():       
                        if version < self.global_table.get(key):
                            ip = function_pos_per_tx[txid][func]
                            self.target_damaged_funcs_per_tx(batch_id, txid, func)
                            self.target_expired_keys_per_node(batch_id, ip, key)
            damaged_funcs[txid] = self.damaged_functions_per_batch[batch_id].get(txid, {})

        downstream_func_table, upstream_func_table = self.get_subjection_table_for_batch(batch_id)
        for k, v in self.expired_keys_per_batch[batch_id].items():
            expired_keys[k] = list(v)

        return expired_keys, len(expired_keys) != 0, damaged_funcs, downstream_func_table, upstream_func_table
    
    # modify global table, and release locks.
    # need to know the whole write set and what ip whey are on.
    def commit_batch(self, batch_id, version: str):
        tx_list = self.tx_list_per_batch[batch_id]
        keys_found = {}
        commit_table = {} # {ip:{tx_id:{key:True}}}
        ip_set = set()

        for tx_id in reversed(tx_list):
            ws = self.write_set_per_batch[batch_id].get(tx_id, {})
            for key, content in ws.items():
                if key in keys_found:
                    continue
                else:
                    keys_found[key] = True
                    ip = content['ip']
                    ip_set.add(ip)
                    if ip not in commit_table:
                        commit_table[ip] = {}
                    if tx_id not in commit_table[ip]:
                        commit_table[ip][tx_id] = {}
                    commit_table[ip][tx_id][key] = True

        jobs = [
            gevent.spawn(self.trigger_worker_commit, batch_id, ip, commit_table[ip], version)
            for ip in ip_set
        ]
        gevent.joinall(jobs)
        
        for key in self.lock_key_set_per_batch[batch_id]:
            self.release_lock(batch_id, key)
        self.acquired_locks.pop(batch_id)
        self.lock_key_set_per_batch.pop(batch_id)
        self.write_set_per_batch.pop(batch_id)
        self.tx_list_per_batch.pop(batch_id)
        self.subjection_table.clean_table_of_batch(batch_id)
        return tx_list
        

    def trigger_worker_commit(self,batch_id, ip, commit_table, version):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        
        print(f"triggering batch_id {batch_id} commit, sending req to {ip}, table:{commit_table}")
        data = {
            'batch_id':batch_id,
            'commit_table': commit_table,
            "version": version
        }
        requests.post(url, json=data)
