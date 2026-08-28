from gevent import monkey

monkey.patch_all()

import argparse
import copy
import csv
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
sys.path.append(str(ROOT_DIR))

from config import config
from config.experiment_logging import activate_experiment
from experiment.common import generate_param
from process_results import summarize_raw_file


RAW_FIELDS = [
    'trace', 'segment_index', 'global_req_id', 'tx_id',
    'configured_abort_prob', 'expected_abort', 'abort_target',
    'retry_abort_seed', 'in_core', 'scheduled_offset', 'fire_offset',
    'status', 'e2e_latency', 'rounds', 'pessimistic', 'occ_retries',
    'submit_timestamp', 'response_timestamp', 'error',
]
PROGRESS_INTERVAL_SECONDS = 10


def assert_experiment_config(workflow):
    errors = []
    if workflow != 'c4':
        errors.append('workflow must be c4')
    if not config.FAST_PATH:
        errors.append('config.FAST_PATH must be True')
    if not config.OPTIMISTIC_REPAIR:
        errors.append('config.OPTIMISTIC_REPAIR must be True')
    if config.ABORT_PROB != 0:
        errors.append('config.ABORT_PROB must be 0')
    if errors:
        raise RuntimeError('; '.join(errors))


def assert_deployed_workflow_schema(workflow):
    response = requests.get(
        f'{config.COUCHDB_URL}/{workflow}_function_info/_all_docs',
        params={'include_docs': 'true'},
    )
    response.raise_for_status()
    documents = {
        row['id']: row['doc']
        for row in response.json().get('rows', [])
        if row.get('doc')
    }
    errors = []
    for index, function_name in enumerate(('f1', 'f2', 'f3', 'f4')):
        expected_from = 'GLOBAL' if index == 0 else f'f{index}'
        schema = documents.get(function_name, {}).get('input', {}).get(
            'retry_abort_func')
        if schema != {'from': expected_from, 'type': 'str'}:
            errors.append(f'{function_name}={schema!r}')
    if errors:
        raise RuntimeError(
            'deployed c4 schema is stale: ' + ', '.join(errors) + '. Run '
            '`python3 src/initializer/initialize.py c4` and restart services.')


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
            f'content_type={response.headers.get("content-type", "")} '
            f'body={body!r}') from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f'gateway returned {type(payload).__name__}, expected object')
    return payload


def append_raw(raw_path, row, write_lock):
    with write_lock:
        with raw_path.open('a', newline='', encoding='utf-8') as output:
            csv.DictWriter(output, fieldnames=RAW_FIELDS).writerow(row)
            output.flush()


def post_request(context, request, parameter, scheduled_offset, in_core):
    target = parameter['f1'].get('retry_abort_func', 'NONE')
    sample_seed = parameter['f1'].get('retry_abort_seed')
    global_req_id = str(request['global_req_id'])
    tx_id = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f'faasnap:trace:{context["trace"]}:{context["segment_index"]}:'
        f'{context["abort_prob"]}:{context["seed"]}:{global_req_id}',
    ))
    gateway_parameter = copy.deepcopy(parameter)
    gateway_parameter['f1'].pop('retry_abort_seed', None)
    submitted = time.time()
    context['waiting'][tx_id] = submitted
    context['submitted'] += 1

    payload = None
    error = ''
    try:
        # Deliberately no timeout: a blocked transaction remains observable.
        response = requests.post(
            f'http://{config.GATEWAY_ADDR}/run',
            json={
                'workflow': context['workflow'],
                'parameters': json.dumps(gateway_parameter),
                'transaction_id': tx_id,
            },
        )
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
    append_raw(context['raw_path'], {
        'trace': context['trace'],
        'segment_index': context['segment_index'],
        'global_req_id': global_req_id,
        'tx_id': tx_id,
        'configured_abort_prob': context['abort_prob'],
        'expected_abort': target != 'NONE',
        'abort_target': target,
        'retry_abort_seed': sample_seed,
        'in_core': in_core,
        'scheduled_offset': scheduled_offset,
        'fire_offset': submitted - context['start_local_time'],
        'status': status,
        'e2e_latency': latency,
        'rounds': payload.get('rounds', '') if payload else '',
        'pessimistic': bool(payload and payload.get('rounds') == 3),
        'occ_retries': payload.get('occ_retries', 0) if payload else 0,
        'submit_timestamp': submitted,
        'response_timestamp': returned,
        'error': error,
    }, context['write_lock'])


def progress_reporter(context):
    while not context['finished']:
        gevent.sleep(PROGRESS_INTERVAL_SECONDS)
        snapshot = {
            'event': 'TRACE_CLIENT_PROGRESS',
            'trace': context['trace'],
            'segment_index': context['segment_index'],
            'configured_abort_prob': context['abort_prob'],
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
        print(
            f'[trace progress] p={context["abort_prob"]:.2f} '
            f'segment={context["segment_index"]} '
            f'scheduled={context["submitted"]} '
            f'completed={context["completed"]} '
            f'waiting={len(context["waiting"])} failed={context["failed"]}',
            flush=True,
        )


def run(args):
    assert_experiment_config(args.workflow)
    assert_deployed_workflow_schema(args.workflow)
    with args.segment.open(encoding='utf-8') as source:
        segment = json.load(source)
    requests_data = sorted(
        segment['requests'], key=lambda item: request_offset(item, segment))
    segment_index = int(segment['segment_index'])
    actual_start, _actual_end = map(float, segment['actual_interval'])
    core_start, core_end = map(float, segment['core_interval'])
    trace_name = args.trace or args.segment.parent.name
    segment_seed = args.seed + segment_index

    parameters = generate_param.generate_workflow_inputs_for_clients(
        'microbenchmark', 1, len(requests_data),
        micro_workflow=args.workflow, zipf_param=args.zipf,
        retry_abort_prob=args.abort_prob, retry_abort_seed=segment_seed,
    )[0]
    log_dir = activate_experiment(
        args.workflow, args.abort_prob, segment_seed,
        metadata={
            'driver': 'open_loop_trace', 'trace': trace_name,
            'segment_index': segment_index, 'request_count': len(requests_data),
            'system_mode': args.system_mode, 'zipf_factor': args.zipf,
        },
    )
    result_dir = SCRIPT_DIR / 'results' / args.system_mode
    raw_dir = result_dir / 'raw_results' / trace_name
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / (
        f'{args.workflow}_segment_{segment_index}_p{args.abort_prob:.2f}_'
        f'{log_dir.name}_raw.csv')
    with raw_path.open('x', newline='', encoding='utf-8') as output:
        csv.DictWriter(output, fieldnames=RAW_FIELDS).writeheader()

    context = {
        'workflow': args.workflow,
        'system_mode': args.system_mode,
        'trace': trace_name,
        'segment_index': segment_index,
        'abort_prob': args.abort_prob,
        'seed': segment_seed,
        'raw_path': raw_path,
        'progress_path': log_dir / 'client_progress.json',
        'write_lock': BoundedSemaphore(),
        'waiting': {}, 'submitted': 0, 'completed': 0, 'failed': 0,
        'finished': False, 'start_local_time': time.time(),
    }
    reporter = gevent.spawn(progress_reporter, context)
    jobs = []
    for request, parameter in zip(requests_data, parameters):
        relative_offset = request_offset(request, segment)
        scheduled_offset = relative_offset - actual_start
        delay = scheduled_offset - (time.time() - context['start_local_time'])
        if delay > 0:
            gevent.sleep(delay)
        in_core = core_start <= relative_offset < core_end
        jobs.append(gevent.spawn(
            post_request, context, request, parameter,
            scheduled_offset, in_core))

    # No join timeout: preserve all blocked requests and server state.
    gevent.joinall(jobs)
    context['finished'] = True
    reporter.kill()
    if context['completed'] != len(requests_data) or context['failed']:
        raise RuntimeError(
            'segment did not finish normally; raw results and progress state '
            'were preserved without writing a summary row')
    summary_path = result_dir / f'summary_results_{trace_name}.csv'
    summarize_raw_file(raw_path, summary_path)
    print(
        f'[trace finished] requests={len(requests_data)} raw={raw_path} '
        f'summary={summary_path}', flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Replay one trace segment as open-loop c4 requests.')
    parser.add_argument('--segment', type=Path, required=True)
    parser.add_argument('--trace', help='trace label; defaults to segment parent')
    parser.add_argument('--workflow', default='c4')
    parser.add_argument('--system-mode', default='hybrid')
    parser.add_argument('--zipf', type=float, default=0.9)
    parser.add_argument('--abort-prob', type=float, required=True)
    parser.add_argument('--seed', type=int, default=20260827)
    args = parser.parse_args()
    if not 0 <= args.abort_prob <= 1:
        parser.error('--abort-prob must be between 0 and 1')
    return args


if __name__ == '__main__':
    run(parse_args())
