from gevent import monkey
monkey.patch_all()
import gevent.lock
import gevent
import requests
import re
import logging
from commiter_repo import Repository
import sys
from gevent import event

sys.path.append('../../config')
import config

COMMITABLE = 1
ABORTED = 2

def extract_ip(address: str) -> str:
    # 使用正则表达式匹配 IP 地址和可选的端口号
    match = re.match(r'^(.*?)(:\d+)?$', address)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid address format")


class AppControllerConcord:
    def __init__(self, workflow_name):
        self.workflow_name = workflow_name
        self.repo = Repository()
        self.function_pos = {}
        self.worker_set = set()
        function_info = self.repo.get_function_info(self.repo.get_all_functions(workflow_name), workflow_name)
        for func, info in function_info.items():
            ip = extract_ip(info['ip'])
            self.function_pos[func] = ip
            self.worker_set.add(ip)
        self.worker_set = list(self.worker_set)
        self.worker_set.sort()
        self.commit_lock = gevent.lock.BoundedSemaphore()
        self.transaction_state  = {}  # {transaction_id: {lock:xx, state:xx, lock_acquired:{}}}
        self.lock_set_per_tx = {}
        self.commit_set_per_tx = {}

    def get_directory_pos(self, key):
        idx = hash(key) % len(self.worker_set)
        return self.worker_set[idx]


    def commit(self, transaction_id, read_set, write_set):
        self.commit_lock.acquire()
        if transaction_id not in self.transaction_state:
            self.transaction_state[transaction_id] = {'lock': gevent.lock.BoundedSemaphore(), 'state': COMMITABLE, 'lock_acquired':0,'lock_cnt':0,'finished':False, "cond":event.Event()}
            self.lock_set_per_tx[transaction_id] = {ip:set() for ip in self.worker_set}
            self.commit_set_per_tx[transaction_id] = {ip:set() for ip in self.worker_set}
        self.transaction_state[transaction_id]['lock'].acquire()
        if self.transaction_state[transaction_id]['state'] != COMMITABLE:
            self.transaction_state.pop(transaction_id, None)
            self.lock_set_per_tx.pop(transaction_id, None)
            self.transaction_state[transaction_id]['lock'].release()
            self.commit_lock.release()
            return   
        workers_for_acquire = set()
        for key in read_set:
            dirctory_pos = self.get_directory_pos(key)
            workers_for_acquire.add(dirctory_pos)
            self.lock_set_per_tx[transaction_id][dirctory_pos].add(key)
        for key, writer_func in write_set.keys():
            dirctory_pos = self.get_directory_pos(key)
            func_pos = self.function_pos[writer_func]
            workers_for_acquire.add(dirctory_pos)
            self.lock_set_per_tx[transaction_id][dirctory_pos].add(key)
            self.commit_set_per_tx[transaction_id][func_pos].add(key)
        logging.info(f"[COMMIT] transaction {transaction_id} acquire locks for keys {self.lock_set_per_tx[transaction_id]} and commit keys {self.commit_set_per_tx[transaction_id]}")
        self.transaction_state[transaction_id]['lock_cnt'] = len(workers_for_acquire)
        self.transaction_state[transaction_id]['lock'].release()
        aborted = self.wait_lock(transaction_id)
        if aborted:
            logging.info(f"[COMMIT] transaction {transaction_id} aborted, release locks.")
            self.transaction_state.pop(transaction_id, None)
            self.lock_set_per_tx.pop(transaction_id, None)
            self.commit_set_per_tx.pop(transaction_id, None)
            self.commit_lock.release()
            return
        logging.info(f"[COMMIT] transaction {transaction_id} acquired locks, commit keys {self.commit_set_per_tx[transaction_id]}")
        jobs = [
            gevent.spawn(requests.post, f"http://{ip}:7000/commit",  {'transaction_id':transaction_id})
            for ip in self.worker_set
        ]
        gevent.joinall(jobs)
        self.acquire_or_unlock(transaction_id, [], False)
        self.commit_lock.release()

    # when unlock: clear the bits, let all 
    def acquire_or_unlock(self, transaction_id, lock):
        acquire_jobs = []
        if lock:
            logging.info(f"[COMMIT] transaction {transaction_id} acquire locks for keys {self.lock_set_per_tx[transaction_id]}")
            for directory_ip, keys in self.lock_set_per_tx[transaction_id]:
                url = f"http://{directory_ip}:6000/concord_lock"
                data = {'transaction_id': transaction_id, 'lock_keys': list(keys), 'lock':lock}
                acquire_jobs.append(gevent.spawn(requests.post, url, data))
        else:
            logging.info(f"[COMMIT] transaction {transaction_id} unlock and commit keys {self.commit_set_per_tx[transaction_id]}")
            for unlock_addr in self.worker_set:
                url = f"http://{unlock_addr}:6000/concord_lock"
                data = {'transaction_id': transaction_id, 'lock_keys': [], 'lock': lock}
                acquire_jobs.append(gevent.spawn(requests.post, url, data))
                acquire_jobs.append(gevent.spawn(requests.post, f"http://{unlock_addr}:7000/stop", {'transaction_id': transaction_id, 'workflow': self.workflow_name}))
        gevent.joinall(acquire_jobs)

    def wait_lock(self, tx_id):
        condition = self.transaction_state[tx_id]['cond']
        condition.clear()
        self.acquire_or_unlock(tx_id, True)
        logging.info(f"[COMMIT] transaction {tx_id} waiting for locks to be acquired.")
        while not self.transaction_state[tx_id]['finished']:
            condition.wait()
        if self.transaction_state[tx_id]['state'] == ABORTED:
            logging.info(f"[COMMIT] transaction {tx_id} aborted, release locks.")
            self.acquire_or_unlock(tx_id, False)
            return True
        return False
    
    def notify_lock(self, tx_id):
        self.transaction_state[tx_id]['lock'].acquire()
        self.transaction_state[tx_id]['lock_acquired'] += 1
        if self.transaction_state[tx_id]['lock_acquired'] == self.transaction_state[tx_id]['lock_cnt']:
            self.transaction_state[tx_id]['finished'] = True
        logging.info(f"[COMMIT] transaction {tx_id} lock acquired, lock_acquired: {self.transaction_state[tx_id]['lock_acquired']}, lock_cnt: {self.transaction_state[tx_id]['lock_cnt']}")
        self.transaction_state[tx_id]['lock'].release()
        self.transaction_state[tx_id]['cond'].set()

    def abort(self, transaction_id):
        self.transaction_state[transaction_id]['lock'].acquire()
        logging.info(f"[COMMIT] transaction {transaction_id} abort, release locks.")
        self.transaction_state[transaction_id]['state'] = ABORTED
        self.transaction_state[transaction_id]['finished'] = True
        self.transaction_state[transaction_id]['lock'].release()
        self.transaction_state[transaction_id]['cond'].set()




