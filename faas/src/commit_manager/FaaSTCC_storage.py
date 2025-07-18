from gevent import monkey
monkey.patch_all()
import gevent.lock
import requests
import sys
from collections import defaultdict
from validator_repo import Repository
from datetime import datetime
import time

sys.path.append('../../config')
import config


GATEWAY_ADDR = config.GATEWAY_ADDR

class FaaSTCC_StorageLayer:
    def __init__(self, workflow_name):
        self.workflow_name = workflow_name
        self.repo = Repository()
        self.nearest_transaction_version = datetime(1000, 1, 1).strftime('%Y-%m-%d %H:%M:%S.%f')
        self.function_pos = {}
        self.worker_set = set()
        function_info = self.repo.get_function_info(self.repo.get_all_functions(workflow_name), workflow_name)
        for func, info in function_info.items():
            self.function_pos[func] = info['ip']
            self.worker_set.add(info['ip'])
        self.worker_set = list(self.worker_set)
        self.commit_lock = gevent.lock.BoundedSemaphore()
        self.versions_list_per_key = defaultdict(list) # {key:[version1, version2, ...]}
        self.key_locks = defaultdict(gevent.lock.BoundedSemaphore)  # {key: lock}
        version_table = self.repo.get_initial_global_table()
        for key, version in version_table.items():
            self.versions_list_per_key[key].append(version)
            self.nearest_transaction_version = max(self.nearest_transaction_version, version)
        print(f"FaaSTCC_StorageLayer initialized verlion list: {self.versions_list_per_key}")
      
    
    def FaaSTCC_get(self, version_target, key):
        promise = ''
        self.key_locks[key].acquire()
        nearest_version = self.nearest_transaction_version
        if key not in self.versions_list_per_key:
            self.key_locks[key].release()
            print(f"Key {key} not found in versions list, returning empty promise.")
            return '', promise
        versions_list = self.versions_list_per_key[key]
        nearest_version = None
        left, right = 0, len(versions_list) - 1
        result_idx = -1
        while left <= right:
            mid = (left + right) // 2
            version_datetime = versions_list[mid]
            if version_datetime <= version_target:
                result_idx = mid  
                left = mid + 1    
            else:
                right = mid - 1  
        if result_idx != -1:
            nearest_version = versions_list[result_idx]
        else:
            print(f"Key {key} with target version {version_target} not found. list: {versions_list}")
            self.key_locks[key].release()
            return '', promise
        promise = versions_list[result_idx + 1] if result_idx + 1 < len(versions_list) else nearest_version
        self.key_locks[key].release()
        return nearest_version, promise

    def FaaSTCC_commit(self, transaction_id, write_set, version, first_run_finish_time):
        start = time.time()
        self.commit_lock.acquire()
        self.nearest_transaction_version = version
        for key in write_set.keys():
            self.key_locks[key].acquire()
            self.versions_list_per_key[key].append(version)
            print(f"FaaSTCC_commit: key {key} updated with version {version}. list: {self.versions_list_per_key[key]}")
            self.key_locks[key].release()
        self.commit_lock.release()
        jobs = [
            gevent.spawn(self.trigger_worker_commit, ip, transaction_id, version)
            for ip in self.worker_set
        ]
        gevent.joinall(jobs)
        timestamps = [[first_run_finish_time, start, time.time() - start]]
        self.notify_gateway(transaction_id, True, timestamps)


    def trigger_worker_commit(self, ip, transaction_id, version):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        data = {
            'transaction_id':transaction_id,
            "version": version
        }
        requests.post(url, json=data)

    def notify_gateway(self, txid, success:bool, timestamps):
        url = 'http://{}/notify'.format(GATEWAY_ADDR)
        data = {
            'transaction_id_list': [[txid]],
            'success': success,
            'timestamps':timestamps
        }
        r = requests.post(url, json=data)
        return r.json() 