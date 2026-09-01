import json
from pathlib import Path
import sys



TRACE_DIR = Path(__file__).resolve().parents[1] / 'experiment/microbenchmark/test7_dynamic_access_set/trace'
sys.path.insert(0, str(TRACE_DIR))

from boki_manifest import create_manifest, load_manifest, load_segment


def test_manifest_is_deterministic_and_covers_every_segment_request(tmp_path):
    segment = {
        'segment_index': 2,
        'base_start_timestamp': 100.0,
        'actual_interval': [0.0, 10.0],
        'core_interval': [1.0, 9.0],
        'requests': [
            {'global_req_id': 'b', 'timestamp': 102.0},
            {'global_req_id': 'a', 'timestamp': 101.0},
        ],
    }
    segment_path = tmp_path / 'segment.json'
    dataset_path = tmp_path / 'keys.json'
    first_path = tmp_path / 'first.jsonl'
    second_path = tmp_path / 'second.jsonl'
    segment_path.write_text(json.dumps(segment), encoding='utf-8')
    dataset_path.write_text(json.dumps([f'key{i}' for i in range(20)]), encoding='utf-8')

    first = create_manifest(segment_path, first_path, dataset_path, 'lowload', 123, 0.9)
    second = create_manifest(segment_path, second_path, dataset_path, 'lowload', 123, 0.9)
    assert first['manifest_sha256'] == second['manifest_sha256']
    assert first_path.read_bytes() == second_path.read_bytes()

    parsed_segment, requests = load_segment(segment_path)
    records, digest = load_manifest(first_path, parsed_segment, requests)
    assert digest == first['manifest_sha256']
    assert set(records) == {'a', 'b'}
    for record in records.values():
        operations = record['key_op_sequence']
        assert list(operations) == ['f1', 'f2', 'f3', 'f4']
        assert sum(len(function_ops) for function_ops in operations.values()) == 12
        assert all(list(function_ops.values()).count('W') == 1 for function_ops in operations.values())

    overwritten = create_manifest(segment_path, first_path, dataset_path, 'lowload', 123, 0.9)
    assert overwritten['manifest_sha256'] == first['manifest_sha256']
