import csv
from pathlib import Path


SUMMARY_FIELDS = [
    'request_count', 'retry_count', 'p50_latency', 'p75_latency',
    'p90_latency', 'p95_latency', 'p99_latency',
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

    measured = [row for row in rows if row['in_core'].lower() == 'true']
    if not measured:
        raise ValueError(f'no core requests in {raw_path}')
    failures = [row for row in measured if row['status'] != 'ok']
    if failures:
        raise ValueError(
            f'{len(failures)} core requests failed; summary not written')

    latencies = [float(row['e2e_latency']) for row in measured]
    summary_row = {
        'request_count': len(measured),
        'retry_count': sum(
            int(row.get('occ_retries') or 0) for row in measured),
        'p50_latency': percentile(latencies, 0.50),
        'p75_latency': percentile(latencies, 0.75),
        'p90_latency': percentile(latencies, 0.90),
        'p95_latency': percentile(latencies, 0.95),
        'p99_latency': percentile(latencies, 0.99),
    }

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    exists = summary_path.exists() and summary_path.stat().st_size > 0
    if exists:
        with summary_path.open(newline='', encoding='utf-8') as source:
            fields = csv.DictReader(source).fieldnames
        if fields != SUMMARY_FIELDS:
            raise ValueError(
                f'incompatible summary header in {summary_path}: {fields}')
    with summary_path.open('a', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(summary_row)


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        raise SystemExit('usage: process_results.py <raw.csv> <summary.csv>')
    summarize_raw_file(sys.argv[1], sys.argv[2])
