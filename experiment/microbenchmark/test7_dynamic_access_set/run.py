import csv
import copy
import json
import multiprocessing
import os
from pathlib import Path
import sys
import threading
import time
import uuid

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[2]
sys.path.append(str(ROOT_DIR))

from config import config
from experiment.common import generate_param
from process_results import summarize_raw_file


ROUND = 100
PROGRESS_INTERVAL_SECONDS = 10
RAW_FIELDS = [
    'tx_id', 'client_id', 'round', 'configured_abort_prob',
    'expected_abort', 'abort_target', 'retry_abort_seed', 'status',
    'e2e_latency', 'rounds', 'pessimistic', 'submit_timestamp',
    'response_timestamp', 'error',
]


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


def append_raw(raw_path, row, write_lock):
    with write_lock:
        with raw_path.open('a', newline='', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=RAW_FIELDS)
            writer.writerow(row)
            output.flush()
            os.fsync(output.fileno())


def update_client_progress(progress, client_id, **updates):
    current = dict(progress.get(client_id, {}))
    current.update(updates)
    progress[client_id] = current


def client_worker(client_id, workflow, abort_prob, parameters, raw_path,
                  write_lock, progress):
    for round_index, parameter in enumerate(parameters, start=1):
        target = parameter['f1'].get('retry_abort_func', 'NONE')
        sample_seed = parameter['f1'].get('retry_abort_seed')
        # UUID5 makes the client-visible tx id reproducible from experiment seed.
        tx_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f'faasnap:{sample_seed}:{client_id}:{round_index}',
        ))
        submit_timestamp = time.time()
        update_client_progress(
            progress, client_id, submitted=round_index,
            currently_waiting_tx_ids=[tx_id],
            last_submit_timestamp=submit_timestamp,
        )
        print(json.dumps({
            'event': 'CLIENT_SUBMIT', 'tx_id': tx_id, 'client_id': client_id,
            'round': round_index, 'configured_probability': abort_prob,
            'abort_target': target, 'retry_abort_seed': sample_seed,
            'timestamp': submit_timestamp,
        }, sort_keys=True), flush=True)

        started = time.time()
        response = None
        error = ''
        try:
            gateway_parameter = copy.deepcopy(parameter)
            gateway_parameter['f1'].pop('retry_abort_seed', None)
            # Deliberately no timeout: a stuck request must preserve the scene.
            http_response = requests.post(
                f'http://{config.GATEWAY_ADDR}/run',
                json={
                    'workflow': workflow,
                    'parameters': json.dumps(gateway_parameter),
                    'transaction_id': tx_id,
                },
            )
            response = http_response.json()
            status = response.get('status', 'error')
            error = response.get('error', '')
        except Exception as exc:
            status = 'client_error'
            error = repr(exc)

        response_timestamp = time.time()
        latency = response_timestamp - started
        aborted = status == 'aborted'
        previous = dict(progress.get(client_id, {}))
        update_client_progress(
            progress,
            client_id,
            returned_committed=previous.get('returned_committed', 0) + (not aborted and status == 'ok'),
            returned_aborted=previous.get('returned_aborted', 0) + aborted,
            currently_waiting_tx_ids=[],
            last_response_timestamp=response_timestamp,
        )
        append_raw(raw_path, {
            'tx_id': tx_id,
            'client_id': client_id,
            'round': round_index,
            'configured_abort_prob': abort_prob,
            'expected_abort': target != 'NONE',
            'abort_target': target,
            'retry_abort_seed': sample_seed,
            'status': status,
            'e2e_latency': response.get('e2e_latency', latency) if response else latency,
            'rounds': response.get('rounds', '') if response else '',
            'pessimistic': bool(response and response.get('rounds', 2) == 3),
            'submit_timestamp': submit_timestamp,
            'response_timestamp': response_timestamp,
            'error': error,
        }, write_lock)
        print(json.dumps({
            'event': 'CLIENT_RESPONSE', 'tx_id': tx_id,
            'client_id': client_id, 'status': status,
            'timestamp': response_timestamp,
        }, sort_keys=True), flush=True)


def progress_reporter(abort_prob, progress, progress_path, stop_event):
    while True:
        clients = {str(key): dict(value) for key, value in progress.items()}
        waiting = [
            tx_id for state in clients.values()
            for tx_id in state.get('currently_waiting_tx_ids', [])
        ]
        snapshot = {
            'event': 'CLIENT_PROGRESS',
            'configured_probability': abort_prob,
            'submitted': sum(state.get('submitted', 0) for state in clients.values()),
            'returned_committed': sum(state.get('returned_committed', 0) for state in clients.values()),
            'returned_aborted': sum(state.get('returned_aborted', 0) for state in clients.values()),
            'currently_waiting_tx_ids': waiting,
            'last_response_timestamp': max(
                (state.get('last_response_timestamp', 0) for state in clients.values()),
                default=0,
            ),
            'timestamp': time.time(),
            'clients': clients,
        }
        temporary_path = progress_path.with_suffix('.tmp')
        temporary_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding='utf-8')
        temporary_path.replace(progress_path)
        print(json.dumps({key: value for key, value in snapshot.items() if key != 'clients'},
                         sort_keys=True), flush=True)
        if stop_event.is_set():
            return
        stop_event.wait(PROGRESS_INTERVAL_SECONDS)


def main():
    if len(sys.argv) != 7:
        raise SystemExit(
            'usage: run.py <workflow> <system_mode> <client_count> '
            '<zipf_param> <abort_prob> <seed>'
        )
    workflow, system_mode = sys.argv[1], sys.argv[2]
    client_count = int(sys.argv[3])
    zipf_param = float(sys.argv[4])
    abort_prob = float(sys.argv[5])
    seed = int(sys.argv[6])
    assert_experiment_config(workflow)

    result_dir = SCRIPT_DIR / 'results' / system_mode
    raw_dir = result_dir / 'raw_results'
    log_dir = SCRIPT_DIR / 'logs'
    raw_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f'{workflow}_{abort_prob:.2f}_raw.csv'
    progress_path = log_dir / f'client_progress_{abort_prob:.2f}.json'
    if raw_path.exists():
        raise RuntimeError(f'refusing to overwrite existing raw result: {raw_path}')
    with raw_path.open('x', newline='', encoding='utf-8') as output:
        csv.DictWriter(output, fieldnames=RAW_FIELDS).writeheader()

    parameters = generate_param.generate_workflow_inputs_for_clients(
        'microbenchmark', client_count, ROUND,
        micro_workflow=workflow, zipf_param=zipf_param,
        retry_abort_prob=abort_prob, retry_abort_seed=seed,
    )
    manager = multiprocessing.Manager()
    progress = manager.dict()
    write_lock = multiprocessing.Lock()
    for client_id in range(client_count):
        progress[client_id] = {
            'submitted': 0, 'returned_committed': 0,
            'returned_aborted': 0, 'currently_waiting_tx_ids': [],
            'last_response_timestamp': 0,
        }

    stop_event = threading.Event()
    reporter = threading.Thread(
        target=progress_reporter,
        args=(abort_prob, progress, progress_path, stop_event),
        daemon=True,
    )
    reporter.start()
    processes = [
        multiprocessing.Process(
            target=client_worker,
            args=(client_id, workflow, abort_prob, parameters[client_id],
                  raw_path, write_lock, progress),
        )
        for client_id in range(client_count)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join()  # Deliberately no timeout.
    stop_event.set()
    reporter.join()
    with raw_path.open(newline='', encoding='utf-8') as source:
        completed_rows = list(csv.DictReader(source))
    normal_completion = (
        len(completed_rows) == client_count * ROUND
        and all(row['status'] in {'ok', 'aborted'} for row in completed_rows)
        and all(process.exitcode == 0 for process in processes)
    )
    if not normal_completion:
        raise RuntimeError(
            'probability point did not complete normally; raw results and '
            'progress state were preserved without writing a summary row'
        )
    summarize_raw_file(raw_path, result_dir / 'summary_results.csv')


if __name__ == '__main__':
    main()
