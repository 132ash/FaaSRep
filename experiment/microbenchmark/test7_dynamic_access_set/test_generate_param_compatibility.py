import csv
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT_DIR))

from experiment.common import generate_param
from experiment.microbenchmark.test7_dynamic_access_set.trace import process_results


class GenerateParamCompatibilityTest(unittest.TestCase):
    def test_dynamic_extension_preserves_legacy_workflow_interfaces(self):
        class FakeRepository:
            @staticmethod
            def get_all_functions(workflow):
                count = int(workflow[1:])
                return [f'f{index}' for index in range(1, count + 1)]

        with mock.patch.object(
                generate_param.repository, 'Repository', FakeRepository), \
                mock.patch.object(
                    generate_param.json, 'load',
                    return_value=[f'key-{index}' for index in range(20)]):
            legacy_c4 = generate_param.generate_micro_benchmark_parameters(
                1, 1, 'c4', 0.9, None)[0][0]['f1']
            legacy_c8 = generate_param.generate_micro_benchmark_parameters(
                1, 1, 'c8', 0.9, None)[0][0]['f1']
            injected_c4 = generate_param.generate_micro_benchmark_parameters(
                1, 1, 'c4', 0.9, None,
                retry_abort_prob=0.5, retry_abort_seed=7)[0][0]['f1']

        self.assertEqual(legacy_c4['retry_abort_func'], 'NONE')
        self.assertNotIn('retry_abort_seed', legacy_c4)
        self.assertNotIn('retry_abort_func', legacy_c8)
        self.assertNotIn('retry_abort_seed', legacy_c8)
        self.assertIsInstance(injected_c4['retry_abort_seed'], int)

    def test_trace_summary_excludes_warmup_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            raw_path = Path(directory) / 'raw.csv'
            summary_path = Path(directory) / 'summary.csv'
            fields = [
                'trace', 'segment_index', 'configured_abort_prob', 'in_core',
                'status', 'e2e_latency', 'submit_timestamp',
                'response_timestamp', 'occ_retries',
            ]
            with raw_path.open('w', newline='', encoding='utf-8') as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                writer.writerows([
                    {
                        'trace': 'lowload', 'segment_index': 1,
                        'configured_abort_prob': 0.5, 'in_core': False,
                        'status': 'ok', 'e2e_latency': 100,
                        'submit_timestamp': 1, 'response_timestamp': 101,
                        'occ_retries': 10,
                    },
                    {
                        'trace': 'lowload', 'segment_index': 1,
                        'configured_abort_prob': 0.5, 'in_core': True,
                        'status': 'ok', 'e2e_latency': 1,
                        'submit_timestamp': 10, 'response_timestamp': 11,
                        'occ_retries': 1,
                    },
                    {
                        'trace': 'lowload', 'segment_index': 1,
                        'configured_abort_prob': 0.5, 'in_core': True,
                        'status': 'ok', 'e2e_latency': 3,
                        'submit_timestamp': 10.5, 'response_timestamp': 14,
                        'occ_retries': 0,
                    },
                ])

            process_results.summarize_raw_file(raw_path, summary_path)
            with summary_path.open(newline='', encoding='utf-8') as source:
                rows = list(csv.DictReader(source))

        self.assertEqual(rows[0]['actual_abort_count'], '1')
        self.assertEqual(rows[0]['success_count'], '2')
        self.assertEqual(rows[0]['success_p50'], '2.0')


if __name__ == '__main__':
    unittest.main()
