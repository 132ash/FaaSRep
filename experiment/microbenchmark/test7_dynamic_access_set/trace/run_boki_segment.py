#!/usr/bin/env python3
"""Replay one prepared manifest against the Boki-style-SN gateway."""
from __future__ import annotations

from gevent import monkey
monkey.patch_all()

import argparse
import csv
from datetime import datetime
import json
from pathlib import Path
import sys
import time
import uuid

import gevent
from gevent.lock import BoundedSemaphore
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[3]
sys.path.insert(0, str(ROOT_DIR))

from config import config
from boki_manifest import load_manifest, load_segment, request_offset
from boki_process_results import summarize_raw_file


SYSTEM = 'BOKI_SN'
RAW_FIELDS = [
    'system', 'trace', 'segment_index', 'global_req_id', 'tx_id', 'in_core',
    'scheduled_offset', 'fire_offset', 'status', 'e2e_latency',
    'workflow_exec_latency', 'lock_wait_latency', 'shadow_get_put_latency',
    'flush_latency', 'db_io_latency', 'unlock_latency', 'rounds', 'retry_count',
    'wait_die_abort_count', 'timeout_abort_count', 'active_abort_count', 'term',
    'lock_request_count', 'immediate_grant_count', 'wait_count',
    'shadow_get_count', 'shadow_hit_count', 'shadow_put_count', 'flushed_key_count',
    'submit_timestamp', 'response_timestamp', 'error',
]
PROGRESS_INTERVAL_SECONDS = 10


def assert_boki_mode():
    if config.SYSTEM_MODE != 'BOKI_SN':
        raise RuntimeError('SYSTEM_MODE must be BOKI_SN in the trace client environment')


def assert_deployed_c4_schema():
    response = requests.get(f'{config.COUCHDB_URL}/c4_function_info/_all_docs',
                            params={'include_docs': 'true'}, timeout=5)
    response.raise_for_status()
    functions = {row.get('doc', {}).get('function_name')
                 for row in response.json().get('rows', [])}
    missing = sorted({'f1', 'f2', 'f3', 'f4'} - functions)
    if missing:
        raise RuntimeError(f'deployed c4 schema is missing {missing}; run initialize.py c4')


def decode_response(response):
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f'gateway returned {type(payload).__name__}, expected object')
    return payload


def append_raw(path, row, lock):
    with lock:
        with path.open('a', newline='', encoding='utf-8') as output:
            csv.DictWriter(output, fieldnames=RAW_FIELDS).writerow(row)
            output.flush()


def write_progress(context):
    snapshot = {
        'system': SYSTEM, 'event': 'TRACE_CLIENT_PROGRESS', 'trace': context['trace'],
        'segment_index': context['segment_index'],
        'scheduled': context['submitted'], 'completed': context['completed'],
        'failed': context['failed'], 'currently_waiting_tx_ids': sorted(context['waiting']),
        'timestamp': time.time(),
    }
    temporary = context['progress_path'].with_suffix('.tmp')
    temporary.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding='utf-8')
    temporary.replace(context['progress_path'])


def progress_reporter(context):
    while not context['finished']:
        gevent.sleep(PROGRESS_INTERVAL_SECONDS)
        write_progress(context)
        print(f'[Boki-SN trace progress] segment={context["segment_index"]} '
              f'scheduled={context["submitted"]} completed={context["completed"]} '
              f'waiting={len(context["waiting"])} failed={context["failed"]}', flush=True)


def metric(payload, name, default=0):
    if not payload:
        return default
    value = payload.get(name, default)
    return default if value is None else value


def post_request(context, request, manifest_record, scheduled_offset, in_core):
    global_req_id = str(request['global_req_id'])
    tx_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                            f'faasnap:boki-sn-trace:{context["trace"]}:'
                            f'{context["segment_index"]}:{context["seed"]}:{global_req_id}'))
    submitted = time.time()
    context['waiting'][tx_id] = submitted
    context['submitted'] += 1
    payload, error = None, ''
    try:
        response = requests.post(f'http://{config.GATEWAY_ADDR}/run', json={
            'workflow': context['workflow'], 'parameters': manifest_record['parameter'],
            'transaction_id': tx_id, 'global_req_id': global_req_id,
        }, timeout=context['request_timeout'] or None)
        payload = decode_response(response)
        status = payload.get('status', 'error')
        error = payload.get('error', '')
    except Exception as exc:
        status, error = 'client_error', repr(exc)
    returned = time.time()
    context['waiting'].pop(tx_id, None)
    context['completed'] += 1
    if status != 'ok':
        context['failed'] += 1
    rounds = int(metric(payload, 'rounds', 0) or 0)
    retry_count = int(metric(payload, 'retry_count', max(0, rounds - 1)) or 0)
    append_raw(context['raw_path'], {
        'system': SYSTEM, 'trace': context['trace'], 'segment_index': context['segment_index'],
        'global_req_id': global_req_id, 'tx_id': tx_id, 'in_core': in_core,
        'scheduled_offset': scheduled_offset, 'fire_offset': submitted - context['start_local_time'],
        'status': status, 'e2e_latency': metric(payload, 'e2e_latency', returned - submitted),
        'workflow_exec_latency': metric(payload, 'workflow_exec_latency'),
        'lock_wait_latency': metric(payload, 'lock_wait_latency'),
        'shadow_get_put_latency': metric(payload, 'shadow_get_put_latency'),
        'flush_latency': metric(payload, 'flush_latency'), 'db_io_latency': metric(payload, 'db_io_latency'),
        'unlock_latency': metric(payload, 'unlock_latency'), 'rounds': rounds, 'retry_count': retry_count,
        'wait_die_abort_count': metric(payload, 'wait_die_abort_count'),
        'timeout_abort_count': metric(payload, 'timeout_abort_count'),
        'active_abort_count': metric(payload, 'active_abort_count'), 'term': metric(payload, 'term'),
        'lock_request_count': metric(payload, 'lock_request_count'),
        'immediate_grant_count': metric(payload, 'immediate_grant_count'),
        'wait_count': metric(payload, 'wait_count'), 'shadow_get_count': metric(payload, 'shadow_get_count'),
        'shadow_hit_count': metric(payload, 'shadow_hit_count'),
        'shadow_put_count': metric(payload, 'shadow_put_count'),
        'flushed_key_count': metric(payload, 'flushed_key_count'),
        'submit_timestamp': submitted,
        'response_timestamp': returned, 'error': error,
    }, context['write_lock'])


def run(args):
    if args.workflow != 'c4':
        raise RuntimeError('Boki-SN trace preparation currently supports only c4')
    assert_boki_mode()
    assert_deployed_c4_schema()
    segment, requests_data = load_segment(args.segment)
    records, _manifest_sha256 = load_manifest(args.manifest, segment, requests_data)
    segment_index = int(segment['segment_index'])
    actual_start, _actual_end = map(float, segment['actual_interval'])
    core_start, core_end = map(float, segment['core_interval'])
    trace_name = args.trace or args.segment.parent.name
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S') + f'_c4_segment{segment_index}_seed{args.seed + segment_index}'
    result_dir = SCRIPT_DIR / 'results' / 'boki_style_single_node'
    raw_dir, progress_dir = result_dir / 'raw_results' / trace_name, result_dir / 'progress'
    raw_dir.mkdir(parents=True, exist_ok=True)
    progress_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f'c4_segment_{segment_index}_{run_id}_raw.csv'
    with raw_path.open('x', newline='', encoding='utf-8') as output:
        csv.DictWriter(output, fieldnames=RAW_FIELDS).writeheader()
    context = {
        'workflow': args.workflow, 'trace': trace_name, 'segment_index': segment_index,
        'seed': args.seed + segment_index, 'request_timeout': args.request_timeout,
        'raw_path': raw_path,
        'progress_path': progress_dir / f'{run_id}.json', 'write_lock': BoundedSemaphore(),
        'waiting': {}, 'submitted': 0, 'completed': 0, 'failed': 0, 'finished': False,
        'start_local_time': time.time(),
    }
    write_progress(context)
    reporter, jobs = gevent.spawn(progress_reporter, context), []
    for request in requests_data:
        offset = request_offset(request, segment)
        scheduled_offset = offset - actual_start
        delay = scheduled_offset - (time.time() - context['start_local_time'])
        if delay > 0:
            gevent.sleep(delay)
        jobs.append(gevent.spawn(post_request, context, request,
                                  records[str(request['global_req_id'])], scheduled_offset,
                                  core_start <= offset < core_end))
    gevent.joinall(jobs)
    context['finished'] = True
    reporter.kill()
    write_progress(context)
    if context['completed'] != len(requests_data) or context['failed']:
        raise RuntimeError('segment did not finish normally; raw results and progress were preserved')
    summary_path = result_dir / f'summary_results_{trace_name}.csv'
    summarize_raw_file(raw_path, summary_path)
    print(f'[Boki-SN trace finished] requests={len(requests_data)} raw={raw_path} summary={summary_path}', flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--segment', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--trace')
    parser.add_argument('--workflow', default='c4')
    parser.add_argument('--seed', type=int, default=20260827)
    parser.add_argument('--request-timeout', type=float, default=300)
    args = parser.parse_args()
    if args.request_timeout < 0:
        parser.error('--request-timeout must be non-negative')
    return args


if __name__ == '__main__':
    run(parse_args())
