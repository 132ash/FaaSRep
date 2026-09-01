#!/usr/bin/env python3
"""Merge OCC trace-result slices into one de-duplicated performance summary.

Each raw result contains a warmup prefix.  Only ``in_core=True`` requests are
included in the aggregate; request IDs are additionally retained once so that
the result remains correct even if core intervals of future slices overlap.
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path


SUMMARY_FIELDS = [
    'trace', 'segment_count', 'request_count', 'success_count',
    'occ_retry_count', 'success_p50', 'success_p75', 'success_p90',
    'success_p99', 'success_throughput',
]
REQUIRED_FIELDS = {
    'trace', 'segment_index', 'global_req_id', 'in_core', 'status',
    'e2e_latency', 'occ_retries', 'submit_timestamp', 'response_timestamp',
}


def percentile(values, probability):
    """Return the linearly interpolated percentile used by process_results."""
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def read_raw_file(raw_path):
    with raw_path.open(newline='', encoding='utf-8') as source:
        reader = csv.DictReader(source)
        missing = REQUIRED_FIELDS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f'{raw_path} is missing columns: {sorted(missing)}')
        return list(reader)


def merge_raw_results(raw_dir):
    """Return one aggregate summary per trace from all ``*_raw.csv`` files."""
    raw_files = sorted(raw_dir.glob('*_raw.csv'))
    if not raw_files:
        raise FileNotFoundError(f'no *_raw.csv files found in {raw_dir}')

    groups = defaultdict(list)
    for raw_path in raw_files:
        rows = read_raw_file(raw_path)
        if not rows:
            print(f'Skipping empty file: {raw_path.name}')
            continue
        traces = {row['trace'] for row in rows}
        if len(traces) != 1:
            raise ValueError(f'{raw_path} mixes trace values')
        groups[traces.pop()].append((raw_path, rows))

    summaries = []
    for trace, files in sorted(groups.items()):
        merged = {}
        total_duration = 0.0
        skipped_warmup = 0
        skipped_duplicates = 0

        for raw_path, rows in files:
            retained_in_file = []
            for row in rows:
                if row['in_core'].lower() != 'true':
                    skipped_warmup += 1
                    continue
                if row['status'] != 'ok':
                    raise ValueError(
                        f'core request {row["global_req_id"]} failed in '
                        f'{raw_path}: {row["status"]}')
                request_id = row['global_req_id']
                if request_id in merged:
                    skipped_duplicates += 1
                    continue
                merged[request_id] = row
                retained_in_file.append(row)

            # Slices were replayed separately, so only add their individual
            # measurement windows instead of using one global time range.
            if retained_in_file:
                start = min(float(row['submit_timestamp']) for row in retained_in_file)
                end = max(float(row['response_timestamp']) for row in retained_in_file)
                total_duration += end - start

        if not merged:
            raise ValueError(f'no core requests remain for trace={trace}')

        merged_rows = list(merged.values())
        latencies = [float(row['e2e_latency']) for row in merged_rows]
        summaries.append({
            'trace': trace,
            'segment_count': len(files),
            'request_count': len(merged_rows),
            'success_count': len(merged_rows),
            'occ_retry_count': sum(
                int(row.get('occ_retries') or 0) for row in merged_rows),
            'success_p50': percentile(latencies, 0.50),
            'success_p75': percentile(latencies, 0.75),
            'success_p90': percentile(latencies, 0.90),
            'success_p99': percentile(latencies, 0.99),
            'success_throughput': len(merged_rows) / total_duration
            if total_duration else 0,
        })
        print(
            f'trace={trace}: files={len(files)}, kept={len(merged_rows)}, '
            f'warmup_skipped={skipped_warmup}, '
            f'core_duplicates_skipped={skipped_duplicates}')
    return summaries


def write_summary(summaries, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', newline='', encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summaries)


def parse_args():
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description='Merge raw OCC trace slices into a performance summary.')
    parser.add_argument(
        '--raw-dir', type=Path,
        default=script_dir / 'results/occ/raw_results/highload',
        help='directory containing *_raw.csv files')
    parser.add_argument(
        '--output', type=Path,
        default=script_dir / 'results/occ/summary_results_highload_merged.csv',
        help='merged summary CSV to write')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    write_summary(merge_raw_results(args.raw_dir), args.output)
    print(f'Wrote merged summary to {args.output}')
