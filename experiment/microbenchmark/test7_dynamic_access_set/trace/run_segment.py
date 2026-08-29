from gevent import monkey

monkey.patch_all()

import argparse
import copy
import csv
from datetime import datetime
import json
from pathlib import Path
import random
import sys
import time
import uuid

import gevent
from gevent.lock import BoundedSemaphore
import numpy as np
import requests


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[3]
sys.path.append(str(ROOT_DIR))

from config import config
from experiment.common import generate_param
from process_results import summarize_raw_file


RAW_FIELDS = [
    'trace', 'segment_index', 'global_req_id', 'tx_id', 'in_core',
    'scheduled_offset', 'fire_offset', 'status', 'e2e_latency', 'rounds',
    'occ_retries', 'submit_timestamp', 'response_timestamp', 'error',
]
PROGRESS_INTERVAL_SECONDS = 10


def request_offset(request, segment):
    if 'relative_time' in request:
        return float(request['relative_time'])
    return float(request['timestamp']) - float(segment['base_start_timestamp'])


def decode_response(response):
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        body = response.text[:200].replace('\n', ' ')
        raise RuntimeError(
            f'gateway returned non-JSON status={response.status_code} '
            f'body={body!r}') from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f'gateway returned {type(payload).__name__}, expected object')
    return payload


def assert_deployed_c4_schema():
    response = requests.get(
        f'{config.COUCHDB_URL}/c4_function_info/_all_docs',
        params={'include_docs': 'true'}, timeout=5)
    response.raise_for_status()
    functions = {
        row.get('doc', {}).get('function_name')
        for row in response.json().get('rows', [])
    }
    missing = sorted({'f1', 'f2', 'f3', 'f4'} - functions)
    if missing:
        raise RuntimeError(
            f'deployed c4 schema is missing {missing}; run '
            '`python3 src/initializer/initialize.py c4` and restart services')


def append_raw(path, row, lock):
    with lock:
        with path.open('a', newline='', encoding='utf-8') as output:
            csv.DictWriter(output, fieldnames=RAW_FIELDS).writerow(row)
            output.flush()


def write_progress(context):
    snapshot = {
        'event': 'TRACE_CLIENT_PROGRESS',
        'trace': context['trace'],
        'segment_index': context['segment_index'],
        'scheduled': context['submitted'],
        'completed': context['completed'],
        'failed': context['failed'],
        'currently_waiting_tx_ids': sorted(context['waiting']),
        'timestamp': time.time(),
    }
    temporary = context['progress_path'].with_suffix('.tmp')
    temporary.write_text(
        json.dumps(snapshot, indent=2, sort_keys=True), encoding='utf-8')
    temporary.replace(context['progress_path'])


def progress_reporter(context):
    while not context['finished']:
        gevent.sleep(PROGRESS_INTERVAL_SECONDS)
        write_progress(context)
        print(
            f'[trace progress] segment={context["segment_index"]} '
            f'scheduled={context["submitted"]} '
            f'completed={context["completed"]} '
            f'waiting={len(context["waiting"])} failed={context["failed"]}',
            flush=True)


def post_request(context, request, parameter, scheduled_offset, in_core):
    global_req_id = str(request['global_req_id'])
    tx_id = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f'faasnap:occ-trace:{context["trace"]}:'
        f'{context["segment_index"]}:{context["seed"]}:{global_req_id}'))
    gateway_parameter = copy.deepcopy(parameter)
    submitted = time.time()
    context['waiting'][tx_id] = submitted
    context['submitted'] += 1

    payload = None
    error = ''
    try:
        timeout = context['request_timeout'] or None
        response = requests.post(
            f'http://{config.GATEWAY_ADDR}/run',
            json={
                'workflow': context['workflow'],
                'parameters': json.dumps(gateway_parameter),
                'transaction_id': tx_id,
            },
            timeout=timeout)
        payload = decode_response(response)
        status = payload.get('status', 'error')
        error = payload.get('error', '')
    except Exception as exc:
        status = 'client_error'
        error = repr(exc)

    returned = time.time()
    context['waiting'].pop(tx_id, None)
    context['completed'] += 1
    if status != 'ok':
        context['failed'] += 1
    latency = payload.get('e2e_latency', returned - submitted) \
        if payload else returned - submitted
    rounds = int(payload.get('rounds', 0) or 0) if payload else 0
    append_raw(context['raw_path'], {
        'trace': context['trace'],
        'segment_index': context['segment_index'],
        'global_req_id': global_req_id,
        'tx_id': tx_id,
        'in_core': in_core,
        'scheduled_offset': scheduled_offset,
        'fire_offset': submitted - context['start_local_time'],
        'status': status,
        'e2e_latency': latency,
        'rounds': rounds,
        'occ_retries': max(0, rounds - 1),
        'submit_timestamp': submitted,
        'response_timestamp': returned,
        'error': error,
    }, context['write_lock'])


def run(args):
    if args.workflow != 'c4':
        raise RuntimeError('the OCC microbenchmark trace supports only c4')
    assert_deployed_c4_schema()
    with args.segment.open(encoding='utf-8') as source:
        segment = json.load(source)
    requests_data = sorted(
        segment['requests'], key=lambda item: request_offset(item, segment))
    segment_index = int(segment['segment_index'])
    actual_start, _actual_end = map(float, segment['actual_interval'])
    core_start, core_end = map(float, segment['core_interval'])
    trace_name = args.trace or args.segment.parent.name
    segment_seed = args.seed + segment_index
    random.seed(segment_seed)
    np.random.seed(segment_seed)

    parameters = generate_param.generate_workflow_inputs_for_clients(
        'microbenchmark', 1, len(requests_data),
        micro_workflow=args.workflow, zipf_param=args.zipf)[0]

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S') + \
        f'_{args.workflow}_segment{segment_index}_seed{segment_seed}'
    result_dir = SCRIPT_DIR / 'results' / 'occ'
    raw_dir = result_dir / 'raw_results' / trace_name
    progress_dir = result_dir / 'progress'
    raw_dir.mkdir(parents=True, exist_ok=True)
    progress_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f'{args.workflow}_segment_{segment_index}_{run_id}_raw.csv'
    with raw_path.open('x', newline='', encoding='utf-8') as output:
        csv.DictWriter(output, fieldnames=RAW_FIELDS).writeheader()

    context = {
        'workflow': args.workflow,
        'trace': trace_name,
        'segment_index': segment_index,
        'seed': segment_seed,
        'request_timeout': args.request_timeout,
        'raw_path': raw_path,
        'progress_path': progress_dir / f'{run_id}.json',
        'write_lock': BoundedSemaphore(),
        'waiting': {}, 'submitted': 0, 'completed': 0, 'failed': 0,
        'finished': False, 'start_local_time': time.time(),
    }
    write_progress(context)
    reporter = gevent.spawn(progress_reporter, context)
    jobs = []
    for request, parameter in zip(requests_data, parameters):
        relative_offset = request_offset(request, segment)
        scheduled_offset = relative_offset - actual_start
        delay = scheduled_offset - (time.time() - context['start_local_time'])
        if delay > 0:
            gevent.sleep(delay)
        jobs.append(gevent.spawn(
            post_request, context, request, parameter, scheduled_offset,
            core_start <= relative_offset < core_end))

    gevent.joinall(jobs)
    context['finished'] = True
    reporter.kill()
    write_progress(context)
    if context['completed'] != len(requests_data) or context['failed']:
        raise RuntimeError(
            'segment did not finish normally; raw results and progress were '
            'preserved without writing a summary row')
    summary_path = result_dir / f'summary_results_{trace_name}.csv'
    summarize_raw_file(raw_path, summary_path)
    print(
        f'[trace finished] requests={len(requests_data)} raw={raw_path} '
        f'summary={summary_path}', flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Replay an open-loop trace segment against OCC c4.')
    parser.add_argument('--segment', type=Path, required=True)
    parser.add_argument('--trace', help='trace label; defaults to segment parent')
    parser.add_argument('--workflow', default='c4')
    parser.add_argument('--zipf', type=float, default=0.9)
    parser.add_argument('--seed', type=int, default=20260827)
    parser.add_argument(
        '--request-timeout', type=float, default=300,
        help='gateway timeout in seconds; use 0 to wait indefinitely')
    args = parser.parse_args()
    if args.request_timeout < 0:
        parser.error('--request-timeout must be non-negative')
    return args


if __name__ == '__main__':
    run(parse_args())
