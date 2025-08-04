from gevent import monkey
monkey.patch_all()
import gevent
import logging
from typing import Any, Dict, List
import requests
from gevent import event
from concord_repo import Repository
from typing import Dict
import sys
from collections import defaultdict
import json
import gevent.lock
sys.path.append('../../config')
import config

Shared = 1
Except = 2
Invalid = 3
SpeculativeRead = 4
SpeculativeWrite = 5

class ConcordCacheAgent:
    def __init__(self, workflow, repo:Repository, node_list:List, self_ip):
        self.workflow = workflow
        self.worker_set = node_list
        self.self_ip = self_ip
        self.worker_set.sort()
        self.directory = {} # {key：{state：xx， sharers：【xx】，lock：xx}}
        self.cache_metadata = {} # key：state：S/E/I, tx_state:[SR/SW], TX_IDs：{txid: true}
        self.access_set_per_tx = defaultdict(dict)  # {transaction_id: {ip: set(keys)}}
        self.repo = repo

    def get_directory_pos(self, key):
        idx = hash(key) % len(self.worker_set)
        return self.worker_set[idx]

    def data_access(self, transaction_id, key, value, mode):
        cache_line = self.cache_metadata.get(key, None)
        if cache_line is None or cache_line['state'] == Invalid:
            # local miss, operate from remote
            # logging.info(f"[CACHE AGENT LOCAL MISS] local miss, operate from remote. key: {key}, mode: {mode}, transaction_id: {transaction_id}")
            success, value = self.data_access_remote(transaction_id, key, value, mode)
        else:
            success, value = self.data_access_local(transaction_id, key, value, mode)
        return success, value

    def data_access_local(self, transaction_id, key, value, mode):
        state = self.cache_metadata[key]['state']
        if mode == 'read':
            # local read hit.
            success = self.local_cacheline_conflict(key, transaction_id, mode, state)
            if not success:
                return False, ''
            return True, self.repo.cache_redis[key]
        else:
            # local write hit.
            if state == Except:
                success = self.local_cacheline_conflict(key, transaction_id, mode, state)
                if not success:
                    return False, ''
                # logging.info(f"[CACHE AGENT WRITE HIT (EXCEPT)] local write hit (except). key: {key}, mode: {mode}, transaction_id: {transaction_id}")
            else:
                # modify shared to Except. let home node invalidate others.
                success = self.local_cacheline_conflict(key, transaction_id, mode, state)
                if not success:
                    return False, ''
                directory_pos = self.get_directory_pos(key)
                # logging.info(f"[CACHE AGENT WRITE HIT (SHARED)] let home invalidate. key: {key}, home {directory_pos} transaction_id: {transaction_id}")
                url = f"http://{directory_pos}:6000/concord_home"
                data = {'mode':'write_hit', 'remote_ip': self.self_ip, 'key': key, 'transaction_id':transaction_id, 'workflow': self.workflow}
                response = requests.post(url, json=data)
                response = response.json()
                if not response['success']:
                    return False, ''
            self.cache_metadata[key]['state'] = Except
            self.cache_metadata[key]['TX_IDs'] = {transaction_id: True}
            self.repo.cache_redis[key] = value
            return True, ''
            
    def data_access_remote(self, transaction_id, key, value, mode):
        directory_url = f"http://{self.get_directory_pos(key)}:6000/concord_home"
        data = {"mode": mode, 'remote_ip': self.self_ip, 'transaction_id': transaction_id, 'key': key, 'workflow': self.workflow}
        response = requests.post(directory_url, json=data)
        response = response.json()
        if not response['success']:
            # logging.info(f"[CACHE ACCESS REMOTE] access failed. ABORT")
            return False, ''
        state = response['state']
        # logging.info(f"[CACHE AGENT VISIT HOME] send request to remote access. key: {key}, mode: {mode}, transaction_id: {transaction_id}, state: {state}")
        if mode == 'read' and state == Except:
            # read after write, retry.
            # logging.info(f"[CACHE AGENT READ AFTER WRITE] read after write. key: {key}, transaction_id: {transaction_id}, just return value.")
            return True, response['value']
        self.cache_metadata[key] = {'state': state, 'TX_IDs': {transaction_id:True}}
        if mode == 'read':
            value = response['value']
            self.repo.cache_redis[key] = value
        else:
            self.repo.cache_redis[key] = value
            value = ''
        return True, value

    def home_serve_remote_read(self, transaction_id, key, remote_ip):
        directory_line  = self.directory.get(key, None)
        value = ''
        state = None

        if directory_line is None:
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] remote read miss. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
            self.directory[key] = {'state': Shared, 'sharers': {remote_ip: True}, 'writer_tx':None,'lock': gevent.lock.BoundedSemaphore()}
            value = self.repo.data_db.get_data_from_db(key)
            return True, value, Shared
            # remote read miss. read from db send back.
        else:
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] waiting lock. remote_ip: {remote_ip}, transaction_id: {transaction_id}, key: {key}")
            directory_line['lock'].acquire()
            state = directory_line['state']
            sharers = directory_line['sharers']
            if state == Shared:
                # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] add remote to shared. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                # remote read hit. add sharer, return value.
                value = self.repo.cache_redis[key] if key in self.cache_metadata else self.repo.data_db.get_data_from_db(key)
                sharers[remote_ip] = True
            else:
                # Except: downgrade the owner to shared. then trigger read miss.
                # RYW in itself, don't modify anything.
                writer_tx = directory_line['writer_tx']
                if writer_tx is not None and writer_tx != transaction_id:
                    # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] remote read: read after write. writer tx: {writer_tx}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                    # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] failed, release lock. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                    directory_line['lock'].release()
                    return False, '', Except
                owner, = directory_line['sharers']
                # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] downgrade owner to shared. key: {key}, owner:{owner}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                directory_url = f"http://{owner}:6000/concord_data"
                data = {'workflow':self.workflow, "mode": 'downgrade',  'key': key}
                response = requests.post(directory_url, json=data)
                response = response.json()
                value = response['value']
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE READ] remote read success. release lock. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
            directory_line['lock'].release()
            return True, value, state

    def home_serve_remote_write(self, transaction_id, key, remote_ip, mode):
        directory_line = self.directory.get(key, None)
        if directory_line is None:
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] remote write miss. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
            self.directory[key] = {'state': Except, 'sharers': {remote_ip: True}, 'writer_tx':transaction_id, 'lock': gevent.lock.BoundedSemaphore()}
            # remote write miss. mark remote_ip as owner. 
        else:
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] waiting lock. remote_ip: {remote_ip}, transaction_id: {transaction_id}, key: {key}")
            directory_line['lock'].acquire()
            if directory_line['state'] == Except:
                writer_tx = directory_line['writer_tx']
                if writer_tx is not None and writer_tx != transaction_id:
                    # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] remote write: write after write. writer tx: {writer_tx}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                    # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] failed, release lock. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                    directory_line['lock'].release()
                    return False, '', Except
            else:
                prev_sharers = directory_line['sharers']
                invalidate_jobs = []
                if mode == 'write_hit':
                    # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] remote_ip: {remote_ip} become only sharer. Invalidate others. key: {key},  transaction_id: {transaction_id}")
                    prev_sharers.pop(remote_ip, None)
                invalidate_result = {}
                for sharer in prev_sharers:
                    invalidate_jobs.append(gevent.spawn(self.home_invalidate_others, key, sharer, transaction_id, invalidate_result))
                gevent.joinall(invalidate_jobs)
                for invalidate_res in invalidate_result.values():
                    if not invalidate_res:
                        # logging.error(f"[CACHE AGENT HOME SERVE REMOTE WRITE] write after read, invalidate others failed. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
                        directory_line['lock'].release()
                        return False,'', Except
            directory_line['state'] = Except
            directory_line['sharers'] = {remote_ip: True}
            directory_line['writer_tx'] = transaction_id
            # logging.info(f"[CACHE AGENT HOME SERVE REMOTE WRITE] remote write success. release lock. key: {key}, remote_ip: {remote_ip}, transaction_id: {transaction_id}")
            directory_line['lock'].release()
        return True, '', Except

    def home_invalidate_others(self, key, sharer, trigger_tx, result_dict):
        url = f"http://{sharer}:6000/concord_data"
        data = {'workflow':self.workflow, "mode":'invalidate', 'key': key, 'trigger_tx': trigger_tx}
        response = requests.post(url, json=data)
        response = response.json()
        result_dict[sharer] = response['success']  

    def invalidated_by_home(self, key, owner_transaction_id):
        cache_line = self.cache_metadata[key]
        if cache_line['state'] == Invalid:
            return True
        current_tx_ids = cache_line['TX_IDs']
        for write_tx_id in current_tx_ids:
            if write_tx_id != owner_transaction_id:
                # logging.info(f"[CACHE AGENT INVALIDATE: WRITE AFTER READ] {write_tx_id} cannot be invalidated by {owner_transaction_id}. Abort transaction {owner_transaction_id}.")
                return False
        cache_line['TX_IDs'] = {}
        cache_line['state'] = Invalid
        # logging.info(f"[CACHE AGENT INVALIDATE BY HOME] invalidate by home. key: {key}, owner_transaction_id: {owner_transaction_id}")
        return True
    
    def downgrade_by_home(self, key):
        # in transaction setting, don't modify the Except to Shared. 
        # logging.info(f"[CACHE AGENT DOWNGRADE BY HOME] origin downgrade by home. Now just return the value to make RYW work.")
        return True, self.repo.cache_redis[key]

    def local_cacheline_conflict(self, key, transaction_id, mode, cache_state):
        cache_line = self.cache_metadata[key]
        current_tx_ids = cache_line['TX_IDs']
        if mode == 'read':
            # don't abort self transaction. add readers.
            if cache_state == Shared:
                # logging.info(f"[CACHE AGENT LOCAL READ CONFLICT] read after read. don't abort self transaction. key: {key}, transaction_id: {transaction_id}")
                current_tx_ids[transaction_id] = True
            else:
                for write_tx_id in current_tx_ids:
                    if write_tx_id != transaction_id:
                        # logging.info(f"[CACHE AGENT LOCAL READ CONFLICT] read after write. prev_tx:{write_tx_id}, abort transaction {transaction_id}. key: {key}")
                        return False
        else:
            # logging.info(f"[CACHE AGENT LOCAL WRITE CONFLICT]  key: {key}, transaction_id: {transaction_id}, current_tx_ids:{current_tx_ids}")
            for write_tx_id in current_tx_ids:
                if write_tx_id != transaction_id:
                    # logging.info(f"[CACHE AGENT LOCAL WRITE CONFLICT] write after write or read. prev_tx:{write_tx_id}, abort transaction {transaction_id}. key: {key}")
                    return False
        return True
    
    def clean_access_set_of_tx(self, transaction_id):
        accessed_keys = self.access_set_per_tx.pop(transaction_id, {})
        for key in accessed_keys:
            if key in self.cache_metadata:
                self.cache_metadata[key]['TX_IDs'].pop(transaction_id, None)
            if key in self.directory:
                directory_line = self.directory[key]
                directory_line['lock'].acquire()
                if directory_line['writer_tx'] == transaction_id:
                    directory_line['writer_tx'] = None
                    directory_line['state'] = Shared
                directory_line['lock'].release()

    def mark_key_access(self, transaction_id, key):
        self.access_set_per_tx[transaction_id][key] = True