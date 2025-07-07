from gevent import monkey
monkey.patch_all()
import gevent.lock
import requests
from collections import defaultdict
from validator_repo import Repository
import sys
from datetime import datetime

sys.path.append('../../config')
import config

class FaaSTCC_StorageLayer:
    def __init__(self, workflow_name, repo:Repository):
        self.workflow_name = workflow_name
        self.repo = repo
        self.nearest_transaction_version = ''
        self.commit_lock = gevent.lock.BoundedSemaphore()
        self.versions_list_per_key = defaultdict(list) # {key:[version1, version2, ...]}
      
    
    def FaaSTCC_get(self, version_target, key):
        promise = ''
        self.commit_lock.acquire()
        if key not in self.versions_list_per_key:
            self.commit_lock.release()
            return '', promise
        versions_list = self.versions_list_per_key[key]
        nearest_version = None
        left, right = 0, len(versions_list) - 1
        result_idx = -1
        while left <= right:
            mid = (left + right) // 2
            version_datetime = datetime.strptime(versions_list[mid], '%Y-%m-%d %H:%M:%S.%f')
            if version_datetime < version_target:
                result_idx = mid  
                left = mid + 1    
            else:
                right = mid - 1  
        if result_idx != -1:
            nearest_version = versions_list[result_idx]
        else:
            self.commit_lock.release()
            return '', promise
        if result_idx + 1 < len(versions_list):
            promise = versions_list[result_idx + 1]
        else:
            promise = self.nearest_transaction_version
        self.commit_lock.release()
        return nearest_version, promise

    def FaaSTCC_commit(self, transaction_id, write_set, version, function_pos, worker_set):
        commit_set_per_ip = defaultdict(list)
        self.commit_lock.acquire()
        self.nearest_transaction_version = version
        for key, func in write_set:
            func_ip = function_pos[func]
            commit_set_per_ip[func_ip].append(key)
            self.versions_list_per_key[key].append(version)
        self.commit_lock.release()
        jobs = [
            gevent.spawn(self.trigger_worker_commit, ip, transaction_id, version, commit_set_per_ip[ip])
            for ip in worker_set
        ]
        gevent.joinall(jobs)

    def trigger_worker_commit(self, ip, transaction_id, version, commit_set):
        if not ip.endswith(":7000"):
            url = f"http://{ip}:7000/commit"
        else:
            url = f"http://{ip}/commit"
        data = {
            'workflow_name': self.workflow_name,
            'transaction_id':transaction_id,
            "version": version,
            "commit_set": commit_set
        }
        requests.post(url, json=data)

    # TODO: FaaSTCC behavior on worker and container, don't send function pos between workers. Keep position infomation on validator.
            