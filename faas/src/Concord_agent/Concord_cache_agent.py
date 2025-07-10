from gevent import monkey
monkey.patch_all()
import gevent
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
        self.cache_metadata = {} # key：state：S/E/I, tx_state:[SR/SW], IDs：{txid: true}
        self.hang_lock_table_per_tx:Dict[str, event.Event] = {}  # {transaction_id: cond}
        self.access_set_per_tx = defaultdict(dict)  # {transaction_id: {ip: set(keys)}}
        self.repo = repo

    def get_directory_pos(self, key):
        idx = hash(key) % len(self.worker_set)
        return self.worker_set[idx]

    def data_access(self, transaction_id, key, mode):
        value = ''
        cache_line = self.cache_metadata.get(key, None)
        if cache_line is None or cache_line['state'] == Invalid:
            # local miss, operate from remote
            value = self.data_access_remote(transaction_id, key, mode)
            self.cache_metadata[key] = {'state': None, 'tx_state':None, 'IDs': {}, 'lock': gevent.lock.BoundedSemaphore()}
        else:
            value = self.data_access_local(transaction_id, key, value, mode)
        self.local_transaction_conflict(key, transaction_id, mode)
        return value

    def data_access_local(self, transaction_id, key, value, mode):
        if mode == 'read':
            # local read hit.
            return self.repo.cache_redis[key]
        else:
            # local write hit.
            state = self.cache_metadata[key]['state']
            if state == Except:
                self.repo.cache_redis[key] = value
            else:
                # let home node invalidate others.
                directory_pos = self.get_directory_pos(key)
                url = f"http://{directory_pos}/concord_home"
                data = {'mode':'write_hit', 'remote_ip': self.self_ip, 'key': key, 'transaction_id':transaction_id, 'workflow': self.workflow}
                requests.post(url, json=data)
                self.cache_metadata[key]['state'] = Except
                return ''
            
    def data_access_remote(self, transaction_id, key, mode):
        directory_url = f"http://{self.get_directory_pos(key)}/concord_home"
        data = {"mode": mode, 'remote_ip': self.self_ip, 'transaction_id': transaction_id, 'key': key, 'workflow': self.workflow}
        response = requests.post(directory_url, json=data).json()
        value = response['value']
        state = response['state']
        self.cache_metadata[key]['state'] = state
        if mode == 'read':
            self.repo.cache_redis[key] = value
        return value

    def home_serve_remote_read(self, transaction_id, key, remote_ip):
        directory_line  = self.directory.get(key, None)
        value = ''
        state = None
        if directory_line is None:
            self.directory[key] = {'state': Except, 'sharers': {remote_ip: True}, 'lock': gevent.lock.BoundedSemaphore()}
            _, value = self.repo.data_db.get_data_from_db(key)
            # remote read miss. read from db send back.
        else:
            directory_line['lock'].acquire()
            state = directory_line['state']
            sharers = directory_line['sharers']
            if state == Shared:
                # remote read hit. add sharer, return value.
                sharers[remote_ip] = True
                value = self.repo.cache_redis[key]
                state = Shared
            else:
                # downgrade the owner to shared. then trigger read miss. 
                owner, = directory_line['sharers']
                directory_url = f"http://{owner}/concord_data"
                data = {'workflow':self.workflow, "mode": 'invalidated',  'key': key, 'trigger_tx': transaction_id}
                requests.post(directory_url, json=data)
                directory_line['state'] = Except
                state = Except
                directory_line['sharers'] = {remote_ip: True}
                _, value = self.repo.data_db.get_data_from_db(key)
            directory_line['lock'].release()
        return value, state

    def home_serve_remote_write(self, transaction_id, key, remote_ip, mode):
        directory_line = self.directory.get(key, None)
        if directory_line is None:
            self.directory[key] = {'state': Except, 'sharers': {remote_ip: True}, 'lock': gevent.lock.BoundedSemaphore()}
            # remote write miss. mark remote_ip as owner. 
        else:
            directory_line['lock'].acquire()
            directory_line['state'] = Except
            prev_sharers = directory_line['sharers']
            directory_line['sharers'] = {remote_ip: True}
            # invalidate prev sharers.
            invalidate_jobs = []
            if mode == 'write_hit':
                prev_sharers.pop(remote_ip, None)
            for sharer in prev_sharers:
                url = f"http://{sharer}/concord_data"
                data = {'workflow':self.workflow, "mode":'invalidate', 'key': key, 'trigger_tx': transaction_id}
                invalidate_jobs.append(gevent.spawn(requests.post, url, json=data))
            gevent.joinall(invalidate_jobs)
        return '', Except

    def invalidate_by_home(self, key, owner_transaction_id):
        cache_line = self.cache_metadata[key]
        prev_tx_ids = cache_line['IDs']
        cache_line['tx_state'] = None
        cache_line['IDs'] = {}
        cache_line['state'] = Invalid
        prev_tx_ids.pop(owner_transaction_id, None)
        self.abort_transactions(prev_tx_ids.keys())
        return '', ''

    def local_transaction_conflict(self, key, transaction_id, mode):
        cache_line = self.cache_metadata[key]
        prev_tx_state = cache_line['tx_state']
        prev_tx_ids = cache_line['IDs']
        self.access_set_per_tx[transaction_id][key] = True
        txs_to_abort = []
        if mode == 'read':
            # don't abort self transaction. add readers.
            if prev_tx_state == SpeculativeRead:
                prev_tx_ids[transaction_id] = True
            else:
                if transaction_id not in prev_tx_ids:
                    # the writer is not itself, change state.
                    txs_to_abort = prev_tx_ids.keys()  # get the first writer
                    cache_line['tx_state'] = SpeculativeRead
                    cache_line['IDs'] = {transaction_id: True}
        else:
            # except for itself, abort all txs.
            prev_tx_ids.pop(transaction_id, None)
            txs_to_abort = prev_tx_ids.keys()
            cache_line['tx_state'] = SpeculativeWrite
            cache_line['IDs'] = {transaction_id: True}
        self.abort_transactions(txs_to_abort)

    def lock_or_unlock_for_commit(self, transaction_id, keys, lock):
        if lock:
            self.hang_lock_table_per_tx[transaction_id] = event.Event()
            self.hang_lock_table_per_tx[transaction_id].clear()
            locks = []
            for key in keys:
                directory_line = self.directory.get(key, None)
                if directory_line is not None:
                    directory_line['lock'].acquire()
                    locks.append(directory_line['lock'])          
            self.hang_lock_table_per_tx[transaction_id].wait()
            for lock in locks:
                lock.release()
            self.hang_lock_table_per_tx.pop(transaction_id, None)
        else:
            for key in self.access_set_per_tx[transaction_id]:
                cacheline = self.cache_metadata.get(key, None)
                if cacheline:
                    cacheline['IDs'].pop(transaction_id, None)
                    if not cacheline['IDs']:
                        cacheline['tx_state'] = None
            if transaction_id in self.hang_lock_table_per_tx:
                self.hang_lock_table_per_tx[transaction_id].set()
    
    def abort_transactions(self, transaction_ids):
        notify_url = "http://{}/notify".format(config.GATEWAY_ADDR)
        payload = {
            'transaction_id_list': [list(transaction_ids)],
            'timestamps': [[0, 0, 0]],  # first_run_finish_time, start_time, validate_time_inside_validator
            'abort': True
        }
        requests.post(notify_url, json=payload)
        return json.dumps({'status': 'ok'})