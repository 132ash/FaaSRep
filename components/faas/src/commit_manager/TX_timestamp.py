from datetime import datetime
from gevent import monkey
monkey.patch_all()
import gevent.lock
from gevent import event

class TimeStampAllocator:

    def __init__(self):
        self.pending_txs = []
        self.lock = gevent.lock.BoundedSemaphore()
        self.wait_condition = {}

    # ensure transactions are assigned timestamps in the order they arrive and validated sequencially.
    def allocate_timestamp(self, tx_id:str):
        self.lock.acquire()
        timestamp = self.get_timestamp()
        self.pending_txs.append(tx_id)
        self.wait_condition[tx_id] = event.Event()
        self.lock.release()
        return timestamp
    
    def wait_for_preceeding_txs(self, tx_id:str):
        condition = self.wait_condition[tx_id] 
        # previous txs are trying to get into the waiting queue of keys.
        while self.pending_txs[0] != tx_id:
            condition.wait()

   # tx finished waiting keys, pop itself, and notify the next tx in the queue.
    def notify_next_tx(self, tx_id:str):
        self.wait_condition.pop(tx_id)
        self.pending_txs.pop(0)
        if len(self.pending_txs) > 0:
            print(f"notifying tx {self.pending_txs[0]}")
            next_tx_id = self.pending_txs[0]
            condition = self.wait_condition[next_tx_id]
            condition.set()
    
    def get_timestamp(self):
        # 获取当前时间，并格式化为字符串，精确到微秒
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        return timestamp


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
    