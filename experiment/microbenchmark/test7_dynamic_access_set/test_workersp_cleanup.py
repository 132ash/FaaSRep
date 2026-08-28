import sys
from pathlib import Path
import types
import unittest
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT_DIR / 'config'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'function_manager'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'workflow_manager'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'gateway'))

# Avoid FunctionManager's module-level repository connection: these tests use
# WorkerSPManager.__new__ and never construct a function manager.
function_manager_stub = types.ModuleType('function_manager')
function_manager_stub.FunctionManager = object
previous_function_manager = sys.modules.get('function_manager')
sys.modules['function_manager'] = function_manager_stub
import workersp
if previous_function_manager is None:
    del sys.modules['function_manager']
else:
    sys.modules['function_manager'] = previous_function_manager
import gateway_repo


class FakeState:
    def __init__(self):
        self.lock = workersp.gevent.lock.BoundedSemaphore()
        self.container_port = {'f1': 21000, 'f2': 24935}
        self.batch_id = 'batch'


class FakeRepository:
    def __init__(self):
        self.cleared_transactions = []

    def clear_mem(self, transaction_id):
        self.cleared_transactions.append(transaction_id)


class WorkerCleanupTest(unittest.TestCase):
    def make_manager(self):
        manager = workersp.WorkerSPManager.__new__(workersp.WorkerSPManager)
        manager.host_addr = '10.0.0.1:7500'
        manager.workflow_name = 'c4'
        # self.func intentionally contains remote functions, matching the
        # repository's current routing-oriented behavior.
        manager.func = ['f1', 'f2']
        manager.function_info = {
            'f1': {'ip': '10.0.0.1:7500'},
            'f2': {'ip': '10.0.0.2:7500'},
        }
        manager.states = {'tx': FakeState()}
        manager.repo = FakeRepository()
        return manager

    def test_clear_mem_contacts_only_local_containers(self):
        manager = self.make_manager()
        response = mock.Mock()
        response.status_code = 200
        with mock.patch.object(
                workersp.requests, 'post', return_value=response) as post:
            self.assertTrue(manager.clear_mem('tx'))

        post.assert_called_once_with(
            'http://127.0.0.1:21000/clear',
            json={'transaction_id': 'tx'}, timeout=2)
        self.assertEqual(manager.repo.cleared_transactions, ['tx'])

    def test_clear_mem_skips_workers_without_transaction_state(self):
        manager = self.make_manager()
        manager.states = {}
        with mock.patch.object(workersp.requests, 'post') as post:
            self.assertFalse(manager.clear_mem('tx'))

        post.assert_not_called()
        self.assertEqual(manager.repo.cleared_transactions, [])

    def test_gateway_targets_only_unique_function_workers(self):
        repository = gateway_repo.Repository.__new__(gateway_repo.Repository)
        repository.couch = {
            'c4_function_info': {
                'f1': {'function_name': 'f1', 'ip': '10.0.0.1:7500'},
                'f2': {'function_name': 'f2', 'ip': '10.0.0.2:7500'},
                'f3': {'function_name': 'f3', 'ip': '10.0.0.1:7500'},
                'metadata': {'kind': 'ignored'},
            },
        }

        self.assertEqual(
            repository.get_function_addrs('c4'),
            ['10.0.0.1:7500', '10.0.0.2:7500'])


if __name__ == '__main__':
    unittest.main()
