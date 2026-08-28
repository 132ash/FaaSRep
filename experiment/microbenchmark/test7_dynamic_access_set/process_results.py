import csv
from pathlib import Path
import sys


SUMMARY_FIELDS = [
    'configured_abort_prob', 'actual_abort_count', 'success_count',
    'success_p50', 'success_p99', 'success_throughput',
]

LEGACY_FIELD_MAP = {
    'configured_abort_prob': 'configured_abort_prob',
    'actual_abort_count': 'aborted',
    'success_count': 'committed',
    'success_p50': 'commit_p50',
    'success_p99': 'commit_p99',
    'success_throughput': 'commit_throughput',
}


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
    commit_latencies = [float(row['e2e_latency']) for row in committed_rows]
    duration = max(float(row['response_timestamp']) for row in committed_rows) - min(
        float(row['submit_timestamp']) for row in rows
    ) if committed_rows else 0
    internal_occ_aborts = sum(
        int(row.get('occ_retries') or 0) for row in rows
    )
    row = {
        'configured_abort_prob': rows[0]['configured_abort_prob'],
        'actual_abort_count': internal_occ_aborts + len(aborted_rows),
        'success_count': len(committed_rows),
        'success_p50': percentile(commit_latencies, .50),
        'success_p99': percentile(commit_latencies, .99),
        'success_throughput': (
            len(committed_rows) / duration if duration else 0
        ),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows = []
    if summary_path.exists():
        with summary_path.open(newline='', encoding='utf-8') as source:
            reader = csv.DictReader(source)
            existing_rows = list(reader)
            existing_fields = reader.fieldnames or []
        if existing_fields != SUMMARY_FIELDS:
            projected_rows = []
            for existing_row in existing_rows:
                projected_rows.append({
                    field: existing_row.get(
                        field, existing_row.get(LEGACY_FIELD_MAP[field], '')
                    )
                    for field in SUMMARY_FIELDS
                })
            with summary_path.open('w', newline='', encoding='utf-8') as output:
                writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
                writer.writeheader()
                writer.writerows(projected_rows)

    exists = summary_path.exists() and summary_path.stat().st_size > 0
    with summary_path.open('a', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('usage: process_results.py <raw.csv> <summary.csv>')
    summarize_raw_file(sys.argv[1], sys.argv[2])
