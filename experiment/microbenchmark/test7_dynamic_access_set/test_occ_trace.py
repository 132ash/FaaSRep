import csv
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[3]
TRACE_DIR = Path(__file__).resolve().parent / 'trace'


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


process_results = load_module(
    'occ_trace_process_results', TRACE_DIR / 'process_results.py')


class SummaryTest(unittest.TestCase):
    def test_summarizes_core_rows_and_occ_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_path = Path(directory) / 'raw.csv'
            summary_path = Path(directory) / 'summary.csv'
            rows = [
                {
                    'trace': 'highload', 'segment_index': '2',
                    'in_core': 'False', 'status': 'ok', 'e2e_latency': '9',
                    'occ_retries': '5', 'submit_timestamp': '0',
                    'response_timestamp': '9',
                },
                {
                    'trace': 'highload', 'segment_index': '2',
                    'in_core': 'True', 'status': 'ok', 'e2e_latency': '1',
                    'occ_retries': '2', 'submit_timestamp': '10',
                    'response_timestamp': '11',
                },
                {
                    'trace': 'highload', 'segment_index': '2',
                    'in_core': 'True', 'status': 'ok', 'e2e_latency': '3',
                    'occ_retries': '1', 'submit_timestamp': '12',
                    'response_timestamp': '15',
                },
            ]
            with raw_path.open('w', newline='', encoding='utf-8') as output:
                writer = csv.DictWriter(output, fieldnames=rows[0])
                writer.writeheader()
                writer.writerows(rows)

            process_results.summarize_raw_file(raw_path, summary_path)

            with summary_path.open(newline='', encoding='utf-8') as source:
                summary = next(csv.DictReader(source))
            self.assertEqual(summary['request_count'], '2')
            self.assertEqual(summary['occ_retry_count'], '3')
            self.assertEqual(float(summary['success_p50']), 2.0)
            self.assertEqual(float(summary['success_throughput']), 0.4)


class ContainerSaturationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(ROOT_DIR / 'src' / 'function_manager'))
        from container import ContainerPool
        from function import Function
        cls.ContainerPool = ContainerPool
        cls.Function = Function

    def test_pool_does_not_reserve_above_limit(self):
        pool = self.ContainerPool(1, 'f1')
        self.assertTrue(pool.check_pool_full_and_occupy())
        self.assertFalse(pool.check_pool_full_and_occupy())

    def test_dispatch_returns_when_pool_is_saturated(self):
        function = self.Function.__new__(self.Function)
        function.rq = [object()]
        function.num_processing = 0

        class SaturatedPool:
            @staticmethod
            def pop():
                return None

        function.container_pool = SaturatedPool()
        function.create_container = lambda: None
        function.dispatch_request()
        self.assertEqual(function.num_processing, 0)
        self.assertEqual(len(function.rq), 1)


class ParameterCompatibilityTest(unittest.TestCase):
    def test_direct_c4_name_uses_microbenchmark_generator(self):
        from experiment.common import generate_param

        expected = [[{'f1': {}}]]
        with mock.patch.object(
                generate_param, 'generate_micro_benchmark_parameters',
                return_value=expected) as generator:
            actual = generate_param.generate_workflow_inputs_for_clients(
                'c4', 1, 1, zipf_param=0.9)

        self.assertIs(actual, expected)
        generator.assert_called_once_with(1, 1, 'c4', 0.9, None)


if __name__ == '__main__':
    unittest.main()
