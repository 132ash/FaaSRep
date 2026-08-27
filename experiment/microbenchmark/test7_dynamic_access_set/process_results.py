import csv
from pathlib import Path
import sys


SUMMARY_FIELDS = [
    'configured_abort_prob', 'submitted', 'terminal', 'committed', 'aborted',
    'actual_abort_ratio', 'actual_abort_selection_ratio',
    'f1_aborts', 'f2_aborts', 'f3_aborts', 'f4_aborts',
    'f1_target_ratio', 'f2_target_ratio', 'f3_target_ratio', 'f4_target_ratio',
    'terminal_throughput', 'commit_throughput', 'abort_throughput',
    'terminal_p50', 'terminal_p99', 'commit_p50', 'commit_p99',
    'abort_p50', 'abort_p99', 'pessimistic_count', 'pessimistic_ratio',
    'complete',
]


def percentile(values, probability):
    if not values:
        return 'NA'
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def summarize_raw_file(raw_path, summary_path):
    raw_path = Path(raw_path)
    summary_path = Path(summary_path)
    with raw_path.open(newline='', encoding='utf-8') as source:
        rows = list(csv.DictReader(source))
    if not rows:
        return
    committed_rows = [row for row in rows if row['status'] == 'ok']
    aborted_rows = [row for row in rows if row['status'] == 'aborted']
    terminal_rows = committed_rows + aborted_rows
    latencies = [float(row['e2e_latency']) for row in terminal_rows]
    commit_latencies = [float(row['e2e_latency']) for row in committed_rows]
    abort_latencies = [float(row['e2e_latency']) for row in aborted_rows]
    duration = max(float(row['response_timestamp']) for row in terminal_rows) - min(
        float(row['submit_timestamp']) for row in rows
    ) if terminal_rows else 0
    target_counts = {
        func: sum(row['abort_target'] == func for row in aborted_rows)
        for func in ('f1', 'f2', 'f3', 'f4')
    }
    submitted = len(rows)
    terminal = len(terminal_rows)
    selected = sum(row['abort_target'] != 'NONE' for row in rows)
    row = {
        'configured_abort_prob': rows[0]['configured_abort_prob'],
        'submitted': submitted, 'terminal': terminal,
        'committed': len(committed_rows), 'aborted': len(aborted_rows),
        'actual_abort_ratio': len(aborted_rows) / submitted,
        'actual_abort_selection_ratio': selected / submitted,
        'terminal_throughput': terminal / duration if duration else 0,
        'commit_throughput': len(committed_rows) / duration if duration else 0,
        'abort_throughput': len(aborted_rows) / duration if duration else 0,
        'terminal_p50': percentile(latencies, .50),
        'terminal_p99': percentile(latencies, .99),
        'commit_p50': percentile(commit_latencies, .50),
        'commit_p99': percentile(commit_latencies, .99),
        'abort_p50': percentile(abort_latencies, .50),
        'abort_p99': percentile(abort_latencies, .99),
        'pessimistic_count': sum(row['pessimistic'].lower() == 'true' for row in terminal_rows),
        'complete': submitted == len(committed_rows) + len(aborted_rows),
        **{f'{func}_aborts': count for func, count in target_counts.items()},
        **{
            f'{func}_target_ratio': (
                sum(source_row['abort_target'] == func for source_row in rows) / selected
                if selected else 0
            )
            for func in ('f1', 'f2', 'f3', 'f4')
        },
    }
    row['pessimistic_ratio'] = row['pessimistic_count'] / terminal if terminal else 0
    exists = summary_path.exists()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open('a', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: process_results.py <raw.csv> <summary.csv>')
    summarize_raw_file(sys.argv[1], sys.argv[2])
