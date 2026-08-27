import json
from pathlib import Path
import time


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[2]
COMPONENT_LOGS = {
    'gateway': ROOT_DIR / 'logging' / 'gateway.log',
    'workersp': ROOT_DIR / 'logging' / 'workersp.log',
    'transaction_sink': ROOT_DIR / 'logging' / 'sink.log',
    'serializer': ROOT_DIR / 'logging' / 'c4_serializer.log',
    'validator_0': ROOT_DIR / 'logging' / 'c4_validator_0.log',
    'validator_1': ROOT_DIR / 'logging' / 'c4_validator_1.log',
    'validator_2': ROOT_DIR / 'logging' / 'c4_validator_2.log',
    'validator_3': ROOT_DIR / 'logging' / 'c4_validator_3.log',
}


def last_nonempty_line(path):
    if not path.exists():
        return None
    with path.open('rb') as source:
        source.seek(0, 2)
        position = source.tell() - 1
        line = b''
        while position >= 0:
            source.seek(position)
            char = source.read(1)
            if char == b'\n' and line:
                break
            if char != b'\n':
                line = char + line
            position -= 1
    return line.decode('utf-8', errors='replace') or None


def main():
    client_snapshots = {}
    waiting = []
    for path in sorted((SCRIPT_DIR / 'logs').glob('client_progress_*.json')):
        try:
            snapshot = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as exc:
            snapshot = {'error': repr(exc)}
        client_snapshots[path.name] = snapshot
        waiting.extend(snapshot.get('currently_waiting_tx_ids', []))
    report = {
        'event': 'READ_ONLY_PROGRESS_INSPECTION',
        'timestamp': time.time(),
        'currently_waiting_tx_ids': waiting,
        'client_snapshots': client_snapshots,
        'component_last_log_line': {
            component: last_nonempty_line(path)
            for component, path in COMPONENT_LOGS.items()
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
