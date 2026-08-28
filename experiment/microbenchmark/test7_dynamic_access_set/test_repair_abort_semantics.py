import csv
import logging
from pathlib import Path
import sys
import tempfile
import unittest


ROOT_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT_DIR / 'config'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'transaction_sink'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'container'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'commit_manager'))
sys.path.insert(0, str(ROOT_DIR / 'src' / 'gateway'))
sys.path.insert(0, str(ROOT_DIR / 'experiment' / 'microbenchmark' /
                       'test7_dynamic_access_set'))

import config
import container_config
import experiment_logging
from Store import Store
import validate_struct
import validator as validator_module
from occ_retry import (
    is_occ_request,
    remove_transaction_dependencies,
    transaction_is_clean,
)
from process_results import SUMMARY_FIELDS, summarize_raw_file
from repair_info import RepairInfo
from transaction_info import (
    RunningTXTable,
    is_injected_retry_abort,
    prepare_occ_retry_parameters,
)


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

    def test_occ_request_is_retried_only_when_dirty(self):
        self.assertTrue(is_occ_request({'retry_abort_func': 'f2'}))
        self.assertFalse(is_occ_request({'retry_abort_func': 'NONE'}))
        self.assertTrue(transaction_is_clean({
            'f1': {'dirty': False}, 'f2': {'dirty': False},
        }))
        self.assertFalse(transaction_is_clean({
            'f1': {'dirty': False}, 'f2': {'dirty': True},
        }))

        dependencies = {
            'batch_sub': {'previous-batch': ['retry', 'keep']},
            'tx_sub': {'previous-tx': ['retry']},
            'whole_tx_sub': {'previous-tx': {'retry': True, 'keep': True}},
            'last_tx': {'retry': 'previous-tx'},
        }
        remove_transaction_dependencies('retry', dependencies)
        self.assertEqual(
            dependencies['batch_sub'], {'previous-batch': ['keep']})
        self.assertEqual(dependencies['tx_sub'], {})
        self.assertEqual(
            dependencies['whole_tx_sub'], {'previous-tx': {'keep': True}})
        self.assertEqual(dependencies['last_tx'], {})

    def test_occ_selection_no_longer_forces_target_dirty(self):
        repair_info = RepairInfo(
            logging.getLogger('repair-info-test'), {'f1': ['END']},
            {'f1': 'worker'})
        repair_info.batch_init('batch')
        repair_info.construct_repair_metadata(
            'batch', {'tx': {'f1': {}}},
            {'tx': {'f1': {
                'dirty': False, 'up_cnt': 0, 'upstream_keys': {},
            }}},
            {'tx': {'f1': {}}}, {'worker'}, ['tx'],
            {'tx': {'f1': '5000'}},
            {'tx': {'retry_abort_func': 'f1'}},
        )
        metadata = repair_info.get_repair_metadata(
            config.OPT_REPAIR, 'batch', 'worker')
        self.assertFalse(metadata['tx']['f1']['dirty'])

    def test_sink_can_remove_occ_retries_before_repair(self):
        state = validate_struct.RepairingBatchState('c4')
        state.register_batch('batch', ['retry', 'keep'], 2)
        state.pessimistic_state_per_batch['batch'].next_txs_after_batch = {
            'later-batch': ['later-tx']
        }
        state.retain_batch_transactions('batch', ['keep'])
        self.assertEqual(state.transaction_list_per_batch['batch'], ['keep'])
        self.assertNotIn('retry', state.optimistic_state_per_transaction)
        self.assertEqual(
            state.tx_finished_table_per_batch['batch'],
            {'total': 1, 'finished': 0})
        self.assertEqual(
            state.pessimistic_state_per_batch['batch'].transaction_list,
            ['keep'])
        self.assertEqual(
            state.pessimistic_state_per_batch['batch'].next_txs_after_batch,
            {'later-batch': ['later-tx']})

        state.register_batch('empty-batch', ['retry-only'], 1)
        state.retain_batch_transactions('empty-batch', [])
        ready, promoted = state.update_subjection_info(
            'empty-batch', {}, {}, {})
        self.assertEqual((ready, promoted), ({}, {}))
        state.clear_opt_table_after_finish(['empty-batch'])
        self.assertNotIn('empty-batch', state.transaction_list_per_batch)
        self.assertNotIn('empty-batch', state.pessimistic_state_per_batch)
        self.assertNotIn('empty-batch', state.tx_finished_table_per_batch)

    def test_validate_survives_concurrent_batch_cleanup(self):
        validator = validator_module.ValidatorProcess.__new__(
            validator_module.ValidatorProcess)
        validator.workflow_name = 'c4'
        validator.logger = logging.getLogger(
            'validate-concurrent-cleanup-test')
        validator.logger.handlers = [logging.NullHandler()]
        validator.logger.propagate = False
        validator.register_lock = validator_module.gevent.lock.BoundedSemaphore()
        validator.tx_list_per_batch = {}
        validator.container_port_per_batch = {}
        validator.read_set_per_batch = {}
        validator.write_set_per_batch = {}
        validator.transaction_metadata_per_batch = {}
        validator.successed_tx_list_per_batch = {}
        validator.aborted_tx_list_per_batch = {}
        validator.aborted_error_per_batch = {}
        validator.retry_tx_list_per_batch = {}
        validator.time_tuple_per_batch = {}
        validator.validate = lambda _batch_id, _batch: ({}, {}, [])

        batch_id = 'batch-cleaned-during-repair'

        class CleaningRepairEngine:
            @staticmethod
            def repair_batch_after_validate(*_args):
                # Model REPAIR_FINISH committing and cleaning the batch while
                # the validation greenlet is yielded in the repair engine.
                validator.tx_list_per_batch.pop(batch_id)

        validator.repair_engine = CleaningRepairEngine()
        batch = {
            'transaction_list': ['tx'],
            'read_set': {'tx': {}},
            'write_set': {'tx': {}},
            'transaction_metadata': {'tx': {}},
            'container_port': {'tx': {}},
        }

        validator.handle_task(
            batch_id, validator_module.VALIDATE, {'batch': batch})
        self.assertNotIn(batch_id, validator.tx_list_per_batch)

    def test_gateway_retry_signal_disables_future_injection(self):
        table = RunningTXTable()
        table.registerTX('c4', 'tx', {})
        table.notifyRetry(['tx'])
        self.assertFalse(table.waitTX('tx'))
        self.assertTrue(table.retryRequested('tx'))
        table.resetTX('tx')
        self.assertFalse(table.retryRequested('tx'))
        self.assertFalse(table.TxFinished('tx'))

        original = {'f1': {'retry_abort_func': 'f2', 'payload_size': 1}}
        retry_parameters = prepare_occ_retry_parameters(original, ['f1'])
        self.assertEqual(retry_parameters['f1']['retry_abort_func'], 'NONE')
        self.assertEqual(original['f1']['retry_abort_func'], 'f2')
        self.assertTrue(is_injected_retry_abort(
            True, 'INJECTED_DYNAMIC_ACCESS_ABORT target=f2'))
        self.assertFalse(is_injected_retry_abort(True, 'real application abort'))

    def test_summary_contains_only_requested_success_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_path = Path(directory) / 'raw.csv'
            summary_path = Path(directory) / 'summary.csv'
            fields = [
                'configured_abort_prob', 'status', 'e2e_latency',
                'submit_timestamp', 'response_timestamp', 'occ_retries',
            ]
            with raw_path.open('w', newline='', encoding='utf-8') as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                writer.writerows([
                    {
                        'configured_abort_prob': '0.5', 'status': 'ok',
                        'e2e_latency': '1', 'submit_timestamp': '10',
                        'response_timestamp': '12', 'occ_retries': '1',
                    },
                    {
                        'configured_abort_prob': '0.5', 'status': 'ok',
                        'e2e_latency': '3', 'submit_timestamp': '10.5',
                        'response_timestamp': '14', 'occ_retries': '0',
                    },
                ])
            summarize_raw_file(raw_path, summary_path)
            with summary_path.open(newline='', encoding='utf-8') as source:
                reader = csv.DictReader(source)
                rows = list(reader)
                self.assertEqual(reader.fieldnames, SUMMARY_FIELDS)
            self.assertEqual(rows[0]['actual_abort_count'], '1')
            self.assertEqual(rows[0]['success_count'], '2')
            self.assertEqual(rows[0]['success_p50'], '2.0')

    def test_experiment_logging_switch_disables_file_handlers(self):
        original = experiment_logging.runtime_config.ENABLE_EXPERIMENT_LOGGING
        try:
            experiment_logging.runtime_config.ENABLE_EXPERIMENT_LOGGING = False
            logger = experiment_logging.make_experiment_logger(
                'disabled-experiment-logger-test', 'test')
            self.assertFalse(logger.handlers)
            self.assertFalse(logger.isEnabledFor(logging.INFO))
        finally:
            experiment_logging.runtime_config.ENABLE_EXPERIMENT_LOGGING = \
                original


if __name__ == '__main__':
    unittest.main()
