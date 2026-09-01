#!/usr/bin/env python3
"""Closed-loop Boki-style-SN c4 data-skewness benchmark.

Each client submits its next workflow only after the preceding request has
returned.  Defaults implement the requested experiment: c4, 32 clients and
Zipf alpha 0.9.  The SUT must already be running in BOKI_SN mode.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import multiprocessing as mp
from pathlib import Path
import random
import sys
import time
import uuid

import numpy as np
import requests


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[2]
print(ROOT_DIR)
sys.path.insert(0, str(ROOT_DIR))

from config import config


SYSTEM = 'BOKI_SN'
DEFAULT_WORKFLOW = 'c4'
DEFAULT_CLIENTS = 32
DEFAULT_ZIPF = 0.9
DEFAULT_ROUNDS = 100
RAW_FIELDS = [
    'system', 'workflow', 'client_id', 'round', 'global_req_id', 'transaction_id', 'status',
    'e2e_latency', 'workflow_exec_latency', 'rounds', 'retry_count',
    'wait_die_abort_count', 'timeout_abort_count', 'active_abort_count', 'term',
    'lock_wait_latency', 'shadow_get_put_latency', 'flush_latency', 'db_io_latency',
    'lock_request_count', 'immediate_grant_count', 'wait_count',
    'shadow_get_count', 'shadow_hit_count', 'shadow_put_count', 'flushed_key_count',
    'submit_timestamp', 'response_timestamp', 'error',
]
SUMMARY_FIELDS = [
    'system', 'workflow', 'client_count', 'rounds_per_client', 'zipf', 'seed',
    'request_count', 'success_count', 'failure_count', 'p50_latency', 'p99_latency',
    'mean_latency', 'closed_loop_throughput', 'retry_count', 'wait_die_abort_count',
    'timeout_abort_count', 'active_abort_count',
]


def percentile(values, probability):
    if not values:
        return 'NA'
    values = sorted(values)
    index = (len(values) - 1) * probability
    lower, upper = int(index), min(int(index) + 1, len(values) - 1)
    fraction = index - lower
    return values[lower] * (1 - fraction) + values[upper] * fraction


def metric(payload, name, default=0):
    if not payload:
        return default
    value = payload.get(name, default)
    return default if value is None else value


def invoke_gateway(workflow, parameter, global_req_id, request_timeout):
    transaction_id = str(uuid.uuid4())
    submitted = time.time()
    payload, error = None, ''
    try:
        response = requests.post(
            f'http://{config.GATEWAY_ADDR}/run',
            json={'workflow': workflow, 'parameters': parameter, 'transaction_id': transaction_id,
                  'global_req_id': global_req_id},
            timeout=request_timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f'gateway returned {type(payload).__name__}, expected object')
        status = payload.get('status', 'error')
        error = payload.get('error', '')
    except Exception as exc:
        status, error = 'client_error', repr(exc)
    returned = time.time()
    return transaction_id, payload, status, error, submitted, returned


def allocate_global_req_id(counter):
    """Assign the request order before the client enters the gateway."""
    with counter.get_lock():
        counter.value += 1
        return counter.value


def worker_task(client_id, workflow, parameters, request_timeout, output_queue, global_req_id_counter):
    """A process-local sequential loop: this is the closed-loop boundary."""
    try:
        for round_index, parameter in enumerate(parameters, 1):
            global_req_id = allocate_global_req_id(global_req_id_counter)
            txid, payload, status, error, submitted, returned = invoke_gateway(
                workflow, parameter, global_req_id, request_timeout)
            rounds = int(metric(payload, 'rounds', 0) or 0)
            output_queue.put(('result', {
                'system': SYSTEM, 'workflow': workflow, 'client_id': client_id,
                'round': round_index, 'global_req_id': global_req_id,
                'transaction_id': txid, 'status': status,
                'e2e_latency': metric(payload, 'e2e_latency', returned - submitted),
                'workflow_exec_latency': metric(payload, 'workflow_exec_latency'),
                'rounds': rounds,
                'retry_count': metric(payload, 'retry_count', max(0, rounds - 1)),
                'wait_die_abort_count': metric(payload, 'wait_die_abort_count'),
                'timeout_abort_count': metric(payload, 'timeout_abort_count'),
                'active_abort_count': metric(payload, 'active_abort_count'),
                'term': metric(payload, 'term'), 'lock_wait_latency': metric(payload, 'lock_wait_latency'),
                'shadow_get_put_latency': metric(payload, 'shadow_get_put_latency'),
                'flush_latency': metric(payload, 'flush_latency'), 'db_io_latency': metric(payload, 'db_io_latency'),
                'lock_request_count': metric(payload, 'lock_request_count'),
                'immediate_grant_count': metric(payload, 'immediate_grant_count'),
                'wait_count': metric(payload, 'wait_count'),
                'shadow_get_count': metric(payload, 'shadow_get_count'),
                'shadow_hit_count': metric(payload, 'shadow_hit_count'),
                'shadow_put_count': metric(payload, 'shadow_put_count'),
                'flushed_key_count': metric(payload, 'flushed_key_count'),
                'submit_timestamp': submitted, 'response_timestamp': returned, 'error': error,
            }))
    finally:
        output_queue.put(('done', client_id))


def generate_parameters(workflow, clients, rounds, zipf, seed):
    # The existing generator is used for parity with other microbenchmarks.
    # Seed both PRNGs because it uses ``random`` and NumPy internally.
    random.seed(seed)
    np.random.seed(seed)
    from experiment.common import generate_param
    return generate_param.generate_workflow_inputs_for_clients(
        'microbenchmark', clients, rounds, micro_workflow=workflow, zipf_param=zipf)


def print_progress(rows, completed, processes, expected_clients, expected_requests, started):
    """Emit a parent-side snapshot without relying on worker-process output."""
    elapsed = max(time.time() - started, 1e-9)
    successes = sum(row['status'] == 'ok' for row in rows)
    failures = len(rows) - successes
    retries = sum(int(row['retry_count'] or 0) for row in rows)
    active_clients = sum(process.is_alive() for process in processes)
    print(
        f'[progress +{elapsed:.1f}s] completed={len(rows)}/{expected_requests} '
        f'ok={successes} failed={failures} clients_done={len(completed)}/{expected_clients} '
        f'clients_active={active_clients} retries={retries} '
        f'rate={len(rows) / elapsed:.2f} req/s',
        flush=True)


def collect_results(processes, output_queue, expected_clients, expected_requests, started, progress_interval):
    rows, completed = [], set()
    next_progress = started
    while len(completed) < expected_clients:
        now = time.time()
        if now >= next_progress:
            print_progress(rows, completed, processes, expected_clients, expected_requests, started)
            next_progress = now + progress_interval
        try:
            kind, payload = output_queue.get(timeout=min(1.0, max(0.01, next_progress - now)))
        except Exception:
            dead = [process.pid for process in processes if process.exitcode not in (None, 0)]
            if dead:
                raise RuntimeError(f'client process failed: {dead}')
            continue
        if kind == 'done':
            completed.add(payload)
        else:
            rows.append(payload)
    print_progress(rows, completed, processes, expected_clients, expected_requests, started)
    for process in processes:
        process.join(timeout=10)
        if process.exitcode not in (0, None):
            raise RuntimeError(f'client process {process.pid} exited with {process.exitcode}')
    return rows


def summarize(rows, client_count, rounds, zipf, seed, elapsed):
    successful = [row for row in rows if row['status'] == 'ok']
    latencies = [float(row['e2e_latency']) for row in successful]
    return {
        'system': SYSTEM, 'workflow': rows[0]['workflow'] if rows else DEFAULT_WORKFLOW,
        'client_count': client_count, 'rounds_per_client': rounds, 'zipf': zipf, 'seed': seed,
        'request_count': len(rows), 'success_count': len(successful),
        'failure_count': len(rows) - len(successful), 'p50_latency': percentile(latencies, 0.50),
        'p99_latency': percentile(latencies, 0.99),
        'mean_latency': sum(latencies) / len(latencies) if latencies else 'NA',
        'closed_loop_throughput': len(successful) / elapsed if elapsed else 0,
        'retry_count': sum(int(row['retry_count'] or 0) for row in rows),
        'wait_die_abort_count': sum(int(row['wait_die_abort_count'] or 0) for row in rows),
        'timeout_abort_count': sum(int(row['timeout_abort_count'] or 0) for row in rows),
        'active_abort_count': sum(int(row['active_abort_count'] or 0) for row in rows),
    }


def append_summary(path, summary):
    exists = path.exists() and path.stat().st_size > 0
    if exists:
        with path.open(newline='', encoding='utf-8') as source:
            if csv.DictReader(source).fieldnames != SUMMARY_FIELDS:
                raise ValueError(f'incompatible summary header in {path}')
    with path.open('a', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(summary)


def run(args):
    if args.workflow != 'c4':
        raise ValueError('this requested closed-loop test is intentionally limited to c4')
    if config.SYSTEM_MODE != 'BOKI_SN':
        raise RuntimeError('set SYSTEM_MODE=BOKI_SN for the client process and use a Boki-SN gateway')
    parameters = generate_parameters(args.workflow, args.clients, args.rounds, args.zipf, args.seed)
    output_queue = mp.Queue(maxsize=max(256, args.clients * 4))
    global_req_id_counter = mp.Value('q', 0)
    processes = [mp.Process(target=worker_task,
                            args=(client_id, args.workflow, parameters[client_id],
                                  args.request_timeout, output_queue, global_req_id_counter))
                 for client_id in range(args.clients)]
    started = time.time()
    for process in processes:
        process.start()
    rows = collect_results(processes, output_queue, args.clients, args.clients * args.rounds,
                           started, args.progress_interval)
    elapsed = time.time() - started
    expected = args.clients * args.rounds
    if len(rows) != expected:
        raise RuntimeError(f'expected {expected} results, received {len(rows)}')

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    result_dir = SCRIPT_DIR / 'results' / 'boki_style_single_node'
    raw_dir = result_dir / 'raw_results'
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f'c4_clients{args.clients}_zipf{args.zipf:.2f}_{run_id}_raw.csv'
    with raw_path.open('x', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=RAW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    summary = summarize(rows, args.clients, args.rounds, args.zipf, args.seed, elapsed)
    append_summary(result_dir / 'summary_results.csv', summary)
    print(json.dumps({'raw_results': str(raw_path), 'summary': summary}, indent=2, default=str))
    if summary['failure_count']:
        raise RuntimeError(f"{summary['failure_count']} closed-loop requests failed; raw output was retained")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflow', default=DEFAULT_WORKFLOW)
    parser.add_argument('--clients', type=int, default=DEFAULT_CLIENTS)
    parser.add_argument('--zipf', type=float, default=DEFAULT_ZIPF)
    parser.add_argument('--rounds', type=int, default=DEFAULT_ROUNDS)
    parser.add_argument('--seed', type=int, default=20260901)
    parser.add_argument('--request-timeout', type=float, default=300)
    parser.add_argument('--progress-interval', type=float, default=5,
                        help='seconds between parent-side progress reports (default: 5)')
    args = parser.parse_args()
    if (args.clients <= 0 or args.rounds <= 0 or args.zipf < 0
            or args.request_timeout <= 0 or args.progress_interval <= 0):
        parser.error('clients, rounds, timeout and progress interval must be positive; zipf must be non-negative')
    return args


if __name__ == '__main__':
    run(parse_args())
