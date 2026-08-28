import json
from pathlib import Path
import time


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parents[2]
LOGGING_ROOT = ROOT_DIR / 'logging'


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
    try:
        run_id = (LOGGING_ROOT / 'ACTIVE_EXPERIMENT').read_text(
            encoding='utf-8').strip()
    except OSError as exc:
        raise SystemExit(f'no active experiment: {exc}')
    run_dir = LOGGING_ROOT / run_id
    progress_path = run_dir / 'client_progress.json'
    try:
        client_snapshot = json.loads(progress_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        client_snapshot = {'error': repr(exc)}
    component_logs = sorted(run_dir.glob('*.log'))
    report = {
        'event': 'READ_ONLY_PROGRESS_INSPECTION',
        'run_id': run_id,
        'log_directory': str(run_dir),
        'timestamp': time.time(),
        'currently_waiting_tx_ids': client_snapshot.get(
            'currently_waiting_tx_ids', []),
        'client_snapshot': client_snapshot,
        'component_last_log_line': {
            path.name: last_nonempty_line(path)
            for path in component_logs
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
