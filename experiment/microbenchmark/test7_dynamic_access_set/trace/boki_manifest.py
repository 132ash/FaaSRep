"""Deterministic c4/Zipf input manifests shared by Boki-SN trace runs.

The manifest is intentionally independent from CouchDB function enumeration.
That makes a request's access sequence stable across Boki-SN and OCC runs.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np


MANIFEST_VERSION = 1
C4_FUNCTIONS = ('f1', 'f2', 'f3', 'f4')
PAYLOAD_SIZE = 4 * 1024


def request_offset(request, segment):
    if 'relative_time' in request:
        return float(request['relative_time'])
    return float(request['timestamp']) - float(segment['base_start_timestamp'])


def load_segment(segment_path: Path):
    with Path(segment_path).open(encoding='utf-8') as source:
        segment = json.load(source)
    requests = sorted(segment['requests'], key=lambda item: request_offset(item, segment))
    ids = [str(item['global_req_id']) for item in requests]
    if len(ids) != len(set(ids)):
        raise ValueError(f'duplicate global_req_id in {segment_path}')
    return segment, requests


def _zipf_cdf(size: int, alpha: float):
    if alpha < 0:
        raise ValueError('zipf must be non-negative')
    if alpha == 0:
        return np.cumsum(np.full(size, 1 / size))
    weights = np.power(np.arange(1, size + 1, dtype=float), -alpha)
    return np.cumsum(weights / np.sum(weights))


def _sample_three_unique(cdf, rng):
    indices = set()
    while len(indices) < 3:
        indices.add(int(np.searchsorted(cdf, rng.random())))
    # A sorted order makes R/R/W assignment independent of set iteration.
    return sorted(indices)


def generate_parameters(request_count: int, dataset: list[str], zipf: float, seed: int):
    if len(dataset) < 3:
        raise ValueError('dataset must contain at least three keys')
    cdf = _zipf_cdf(len(dataset), zipf)
    rng = np.random.default_rng(seed)
    parameters = []
    for _ in range(request_count):
        accesses = {}
        for function_name in C4_FUNCTIONS:
            keys = [dataset[index] for index in _sample_three_unique(cdf, rng)]
            accesses[function_name] = {keys[0]: 'R', keys[1]: 'R', keys[2]: 'W'}
        parameters.append({
            'f1': {
                'payload_size': PAYLOAD_SIZE,
                'keys': json.dumps(accesses, separators=(',', ':'), sort_keys=True),
            }
        })
    return parameters


def _sha256_bytes(data: bytes):
    return hashlib.sha256(data).hexdigest()


def create_manifest(segment_path: Path, output_path: Path, dataset_path: Path, trace: str,
                    seed: int, zipf: float):
    """Write JSONL records and a metadata sidecar; never starts a workload."""
    segment_path = Path(segment_path)
    output_path = Path(output_path)
    dataset_path = Path(dataset_path)
    segment, requests = load_segment(segment_path)
    dataset = json.loads(dataset_path.read_text(encoding='utf-8'))
    segment_index = int(segment['segment_index'])
    segment_seed = int(seed) + segment_index
    parameters = generate_parameters(len(requests), dataset, float(zipf), segment_seed)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    hasher = hashlib.sha256()
    with output_path.open('w', encoding='utf-8') as output:
        for request, parameter in zip(requests, parameters):
            key_ops = json.loads(parameter['f1']['keys'])
            record = {
                'manifest_version': MANIFEST_VERSION,
                'trace': trace,
                'segment_index': segment_index,
                'global_req_id': str(request['global_req_id']),
                'parameter': parameter,
                'key_op_sequence': key_ops,
            }
            record['parameter_sha256'] = _sha256_bytes(
                json.dumps(parameter, sort_keys=True, separators=(',', ':')).encode('utf-8'))
            line = json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n'
            output.write(line)
            hasher.update(line.encode('utf-8'))

    metadata = {
        'manifest_version': MANIFEST_VERSION,
        'trace': trace,
        'segment_index': segment_index,
        'request_count': len(requests),
        'zipf': float(zipf),
        'base_seed': int(seed),
        'segment_seed': segment_seed,
        'dataset_path': str(dataset_path),
        'dataset_sha256': _sha256_bytes(dataset_path.read_bytes()),
        'segment_path': str(segment_path),
        'segment_sha256': _sha256_bytes(segment_path.read_bytes()),
        'manifest_sha256': hasher.hexdigest(),
    }
    metadata_path = output_path.with_suffix('.metadata.json')
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return metadata


def load_manifest(manifest_path: Path, segment, requests):
    manifest_path = Path(manifest_path)
    records = {}
    hasher = hashlib.sha256()
    with manifest_path.open(encoding='utf-8') as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            hasher.update(line.encode('utf-8'))
            record = json.loads(line)
            request_id = str(record['global_req_id'])
            if request_id in records:
                raise ValueError(f'duplicate global_req_id {request_id} at line {line_number}')
            records[request_id] = record

    expected = {str(request['global_req_id']) for request in requests}
    if set(records) != expected:
        missing = sorted(expected - set(records))[:5]
        extra = sorted(set(records) - expected)[:5]
        raise ValueError(f'manifest request set differs; missing={missing}, extra={extra}')
    expected_index = int(segment['segment_index'])
    for record in records.values():
        if record.get('manifest_version') != MANIFEST_VERSION:
            raise ValueError('unsupported manifest version')
        if int(record.get('segment_index', -1)) != expected_index:
            raise ValueError('manifest segment_index does not match segment')
        if 'parameter' not in record:
            raise ValueError('manifest record has no parameter')
    return records, hasher.hexdigest()
