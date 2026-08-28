import logging
from pathlib import Path
import sys
import unittest


ROOT_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT_DIR / 'config'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'transaction_sink'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'container'))

import config
import container_config
from Store import Store
import validate_struct


class FakeStore:
    def __init__(self, is_optimistic_repair):
        self.is_optimistic_repair = is_optimistic_repair
        self.metadata = {}
        self.aborted = False

    def fetch_input(self):
        return {
            'retry_abort_func': 'f2',
            'keys': '{"f2": {}}',
            'payload_size': 1,
        }

    def set_transaction_metadata(self, key, value):
        self.metadata[key] = value

    def abort_tx(self, message):
        self.aborted = True
        raise RuntimeError(message)

    def get(self, key):
        raise AssertionError('unexpected read')

    def put(self, key, value):
        raise AssertionError('unexpected write')

    def ret(self, output):
        self.output = output


class RepairAbortSemanticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_path = (
            ROOT_DIR / 'scripts' / 'init' / 'micro_benchmark' /
            'microbenchmark_func' / 'main.py'
        )
        cls.function_source = source_path.read_text(encoding='utf-8')
        validate_struct.logger = logging.getLogger(
            'test_repair_abort_semantics')
        validate_struct.logger.handlers = [logging.NullHandler()]
        validate_struct.logger.propagate = False

    def run_function(self, is_optimistic_repair):
        store = FakeStore(is_optimistic_repair)
        namespace = {'function_name': 'f2', 'store': store}
        exec(self.function_source, namespace)
        namespace['main']()
        return store

    def test_injected_abort_only_runs_during_optimistic_repair(self):
        for phase in ('initial execution', 'pessimistic repair'):
            with self.subTest(phase=phase):
                store = self.run_function(False)
                self.assertFalse(store.aborted)

        with self.assertRaisesRegex(
                RuntimeError, 'INJECTED_DYNAMIC_ACCESS_ABORT target=f2'):
            self.run_function(True)

    def test_store_exposes_the_exact_repair_mode(self):
        metadata = {
            'read_set': {}, 'write_set': {}, 'RYW_subjection': {},
            'keys_from_RYW': {}, 'keys_from_upstream': {},
        }
        cases = (
            (False, None, False),
            (True, container_config.OPT_REPAIR, True),
            (True, container_config.PESSI_REPAIR, False),
        )
        for is_repair, repair_mode, expected in cases:
            with self.subTest(
                    is_repair=is_repair, repair_mode=repair_mode):
                store = Store.__new__(Store)
                store.runtime_init(
                    {}, {}, is_repair, repair_mode, 'tx', metadata.copy())
                self.assertEqual(store.is_optimistic_repair, expected)

    def test_terminal_optimistic_abort_wins_late_promotion(self):
        state = validate_struct.RepairingBatchState('c4')
        batch_id = 'batch'
        predecessor = 'predecessor'
        aborted_successor = 'aborted-successor'
        state.register_batch(
            batch_id, [predecessor, aborted_successor], batch_size=2)
        state.update_subjection_info(
            batch_id, {}, {predecessor: [aborted_successor]}, {})

        finished, cascaded = state.after_transaction_finish(
            batch_id, config.OPT_REPAIR, aborted_successor, config.ABORTED,
            False, repair_epoch=1, attempt_id='optimistic-abort')
        self.assertEqual(finished, {})
        self.assertEqual(cascaded, {})

        # Simulate a predecessor abort racing after the successor's own abort.
        successor_state = state.optimistic_state_per_transaction[
            aborted_successor]
        successor_state.need_pessimistic_repair = True

        finished, cascaded = state.after_transaction_finish(
            batch_id, config.OPT_REPAIR, predecessor, config.REPAIRED,
            False, repair_epoch=1, attempt_id='predecessor-finish')

        self.assertEqual(cascaded, {})
        self.assertTrue(finished[batch_id]['batch_finished'])
        self.assertEqual(
            finished[batch_id]['aborted_txs'], [aborted_successor])
        self.assertEqual(finished[batch_id]['pessi_repair_txs'], [])


if __name__ == '__main__':
    unittest.main()
