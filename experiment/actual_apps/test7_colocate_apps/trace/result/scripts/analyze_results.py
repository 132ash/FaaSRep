import argparse
import csv
import json
import math
import os


ROUND_COLUMNS = [
    'Average Rounds',
    'P50 Rounds',
    'P99 Rounds',
    'Max Rounds',
    'Retried Requests',
    'Retry Ratio',
]


def percentile(sorted_values, p):
    if not sorted_values:
        return 0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def build_round_stats(ids, fallback_rounds=None):
    rounds = [
        info.get('rounds')
        for info in ids.values()
        if isinstance(info.get('rounds'), (int, float))
    ]
    if not rounds and fallback_rounds:
        rounds = [r for r in fallback_rounds if isinstance(r, (int, float))]
    if not rounds:
        return {
            'Average Rounds': 0,
            'P50 Rounds': 0,
            'P99 Rounds': 0,
            'Max Rounds': 0,
            'Retried Requests': 0,
            'Retry Ratio': 0,
        }

    sorted_rounds = sorted(rounds)
    retried_requests = sum(1 for r in rounds if r > 1)
    return {
        'Average Rounds': sum(rounds) / len(rounds),
        'P50 Rounds': percentile(sorted_rounds, 50),
        'P99 Rounds': percentile(sorted_rounds, 99),
        'Max Rounds': max(rounds),
        'Retried Requests': retried_requests,
        'Retry Ratio': retried_requests / len(rounds),
    }


def analyze(file_path, output_csv=None, workflow_name=None):
    print(f"Analyzing {file_path}...")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    latencies = data.get('latencies', [])
    ids = data.get('ids', {})
    valid_latencies = [l for l in latencies if l is not None]
    if not valid_latencies:
        print("No valid latency data.")
        return

    sorted_latencies = sorted(valid_latencies)
    avg_latency = sum(valid_latencies) / len(valid_latencies)
    p50_latency = percentile(sorted_latencies, 50)
    p99_latency = percentile(sorted_latencies, 99)
    round_stats = build_round_stats(ids, data.get('rounds', []))

    print(f"Total Requests: {len(valid_latencies)}")
    print(f"Average Latency: {avg_latency:.4f} s")
    print(f"P50 Latency: {p50_latency:.4f} s")
    print(f"P99 Latency: {p99_latency:.4f} s")
    print("Rounds:")
    for key in ROUND_COLUMNS:
        value = round_stats[key]
        if key in ('Retried Requests', 'Max Rounds'):
            print(f"  {key}: {int(value)}")
        else:
            print(f"  {key}: {value:.6f}")

    start_times = []
    end_times = []
    for info in ids.values():
        if 'st' in info and 'ed' in info:
            start_times.append(info['st'])
            end_times.append(info['ed'])

    duration = 0
    throughput = 0
    if start_times and end_times:
        min_start = min(start_times)
        max_end = max(end_times)
        duration = max_end - min_start
        if duration > 0:
            throughput = len(valid_latencies) / duration
            print(f"Experiment Duration: {duration:.2f} s")
            print(f"Average Throughput: {throughput:.2f} req/s")
        else:
            print("Duration is zero or negative, cannot calculate throughput.")
    else:
        print("No timing information found in 'ids'.")

    if output_csv:
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        file_exists = os.path.isfile(output_csv)
        with open(output_csv, 'a', newline='') as csvfile:
            first_col = 'workflow' if workflow_name else 'File'
            fieldnames = [
                first_col,
                'Total Requests',
                'Average Latency',
                'P50 Latency',
                'P99 Latency',
                'Experiment Duration',
                'Average Throughput',
            ] + ROUND_COLUMNS
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()

            row = {
                first_col: workflow_name if workflow_name else os.path.basename(file_path),
                'Total Requests': len(valid_latencies),
                'Average Latency': f"{avg_latency:.4f}",
                'P50 Latency': f"{p50_latency:.4f}",
                'P99 Latency': f"{p99_latency:.4f}",
                'Experiment Duration': f"{duration:.2f}",
                'Average Throughput': f"{throughput:.2f}",
                'Average Rounds': f"{round_stats['Average Rounds']:.6f}",
                'P50 Rounds': f"{round_stats['P50 Rounds']:.6f}",
                'P99 Rounds': f"{round_stats['P99 Rounds']:.6f}",
                'Max Rounds': int(round_stats['Max Rounds']),
                'Retried Requests': int(round_stats['Retried Requests']),
                'Retry Ratio': f"{round_stats['Retry Ratio']:.6f}",
            }
            writer.writerow(row)
        print(f"Results saved to {output_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, required=True, help='Path to merged result JSON file')
    parser.add_argument('--output-csv', type=str, help='Path to output CSV file')
    parser.add_argument('--workflow', type=str, help='Workflow name for CSV output')
    args = parser.parse_args()

    analyze(args.file, args.output_csv, args.workflow)
