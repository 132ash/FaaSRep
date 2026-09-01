#!/usr/bin/env python3
"""Merge core requests from trace-result slices by abort probability.

Each raw result contains the slice's warmup prefix as well as its measured
core.  Only core requests are eligible for the aggregate, and global request
IDs are retained once per (trace, abort probability) pair.  This makes the
script safe when consecutive slices overlap in their warmup portions.
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path


SUMMARY_FIELDS = [
    'trace', 'configured_abort_prob', 'actual_abort_count', 'success_count',
    'success_p50', 'success_p75', 'success_p90', 'success_p99',
    'success_throughput',
]
REQUIRED_FIELDS = {
    'trace', 'global_req_id', 'configured_abort_prob', 'in_core', 'status',
    'e2e_latency', 'occ_retries', 'submit_timestamp', 'response_timestamp',
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


def read_raw_file(raw_path):
    with raw_path.open(newline='', encoding='utf-8') as source:
        reader = csv.DictReader(source)
        fields = set(reader.fieldnames or [])
        missing = REQUIRED_FIELDS - fields
        if missing:
            raise ValueError(f'{raw_path} is missing columns: {sorted(missing)}')
        return list(reader)


def merge_raw_results(raw_dir):
    """Return one aggregate summary per (trace, abort probability) pair."""
    raw_files = sorted(raw_dir.glob('*_raw.csv'))
    if not raw_files:
        raise FileNotFoundError(f'no *_raw.csv files found in {raw_dir}')

    groups = defaultdict(list)
    for raw_path in raw_files:
        rows = read_raw_file(raw_path)
        if not rows:
            print(f'Skipping empty file: {raw_path.name}')
            continue

        keys = {(row['trace'], row['configured_abort_prob']) for row in rows}
        if len(keys) != 1:
            raise ValueError(f'{raw_path} mixes trace or abort-probability values')
        groups[keys.pop()].append((raw_path, rows))

    summaries = []
    for (trace, abort_prob), files in sorted(groups.items()):
        merged = {}
        total_duration = 0.0
        skipped_warmup = 0
        skipped_duplicates = 0

        for raw_path, rows in files:
            core_rows = []
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
                core_rows.append(row)

            # Add durations slice-by-slice: the slices may have been replayed
            # days apart, so one global min/max timestamp would be meaningless.
            if core_rows:
                start = min(float(row['submit_timestamp']) for row in core_rows)
                end = max(float(row['response_timestamp']) for row in core_rows)
                total_duration += end - start

        if not merged:
            raise ValueError(f'no core requests remain for trace={trace}, p={abort_prob}')

        merged_rows = list(merged.values())
        latencies = [float(row['e2e_latency']) for row in merged_rows]
        summaries.append({
            'trace': trace,
            'configured_abort_prob': abort_prob,
            'actual_abort_count': sum(
                int(row.get('occ_retries') or 0) for row in merged_rows),
            'success_count': len(merged_rows),
            'success_p50': percentile(latencies, 0.50),
            'success_p75': percentile(latencies, 0.75),
            'success_p90': percentile(latencies, 0.90),
            'success_p99': percentile(latencies, 0.99),
            'success_throughput': len(merged_rows) / total_duration
            if total_duration else 0,
        })
        print(
            f'trace={trace} p={abort_prob}: files={len(files)}, '
            f'kept={len(merged_rows)}, warmup_skipped={skipped_warmup}, '
            f'duplicates_skipped={skipped_duplicates}')
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
        description='Merge raw trace slices into one row per abort probability.')
    parser.add_argument(
        '--raw-dir', type=Path,
        default=script_dir / 'results/FaaSRep/raw_results/highload',
        help='directory containing *_raw.csv files')
    parser.add_argument(
        '--output', type=Path,
        default=script_dir / 'results/FaaSRep/summary_results_highload_merged.csv',
        help='merged summary CSV to write')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    summaries = merge_raw_results(args.raw_dir)
    write_summary(summaries, args.output)
    print(f'Wrote {len(summaries)} aggregate rows to {args.output}')
