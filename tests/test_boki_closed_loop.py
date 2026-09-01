import csv
import importlib.util
from pathlib import Path


RUNNER_PATH = Path(__file__).resolve().parents[1] / 'experiment/microbenchmark/test3_data_skewness/run.py'
spec = importlib.util.spec_from_file_location('boki_closed_loop', RUNNER_PATH)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def row(status='ok', latency=1.0, retries=0):
    return {
        'workflow': 'c4', 'status': status, 'e2e_latency': latency,
        'retry_count': retries, 'wait_die_abort_count': retries,
        'timeout_abort_count': 0, 'active_abort_count': 0,
    }


def test_closed_loop_summary_counts_successes_failures_and_retries():
    summary = runner.summarize(
        [row('ok', 1.0, 1), row('ok', 3.0, 2), row('error', 9.0, 0)],
        client_count=32, rounds=100, zipf=0.9, seed=1, elapsed=2.0)
    assert summary['request_count'] == 3
    assert summary['success_count'] == 2
    assert summary['failure_count'] == 1
    assert summary['p50_latency'] == 2.0
    assert summary['closed_loop_throughput'] == 1.0
    assert summary['retry_count'] == 3
    assert summary['wait_die_abort_count'] == 3


def test_summary_file_has_stable_schema(tmp_path):
    summary = runner.summarize([row()], 32, 100, 0.9, 1, 1.0)
    path = tmp_path / 'summary.csv'
    runner.append_summary(path, summary)
    with path.open(newline='', encoding='utf-8') as source:
        assert csv.DictReader(source).fieldnames == runner.SUMMARY_FIELDS
