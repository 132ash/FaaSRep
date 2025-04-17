from datetime import datetime
import logging
from gevent import monkey
monkey.patch_all()
import gevent.lock
from gevent import event

class TimeStampAllocator:

    def __init__(self):
        self.pending_batches = []
        self.lock = gevent.lock.BoundedSemaphore()
        self.wait_condition = {}

    # ensure transactions are assigned timestamps in the order they arrive and validated sequencially.
    def allocate_timestamp(self, batch_id:str):
        self.lock.acquire()
        timestamp = self.get_timestamp()
        self.pending_batches.append(batch_id)
        self.wait_condition[batch_id] = event.Event()
        self.lock.release()
        return timestamp
    
    def wait_for_preceeding_batches(self, batch_id:str):
        condition = self.wait_condition[batch_id] 
        # previous batchs are trying to get into the waiting queue of keys.
        while self.pending_batches[0] != batch_id:
            condition.wait()

   # batch finished waiting keys, pop itself, and notify the next batch in the queue.
    def notify_next_batch(self, batch_id:str):
        self.wait_condition.pop(batch_id)
        self.pending_batches.pop(0)
        if len(self.pending_batches) > 0:
            logging.info(f"finished adding lock. notifying batch {self.pending_batches[0]}")
            next_batch_id = self.pending_batches[0]
            condition = self.wait_condition[next_batch_id]
            condition.set()
    
    def get_timestamp(self):
        # 获取当前时间，并格式化为字符串，精确到微秒
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        return timestamp


class BatchVersion:
    def __init__(self, commit_timestamp: str):
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
        # 将 batchVersion 对象转换为紧凑的字符串
        return f"{self.commit_timestamp}"

    @classmethod
    def from_string(cls, version_str: str):
        return cls(version_str)
    