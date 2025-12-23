import json
import argparse
import os
import math
import csv

def analyze(file_path, output_csv=None, workflow_name=None):
    print(f"Analyzing {file_path}...")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    latencies = data.get('latencies', [])
    ids = data.get('ids', {})
    
    if not latencies:
        print("No latency data found.")
        return

    # Latency analysis
    # Filter out None or invalid latencies if any
    valid_latencies = [l for l in latencies if l is not None]
    
    if not valid_latencies:
        print("No valid latency data.")
        return

    avg_latency = sum(valid_latencies) / len(valid_latencies)
    
    # Sort for percentiles
    sorted_latencies = sorted(valid_latencies)
    def get_percentile(p):
        k = (len(sorted_latencies) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_latencies[int(k)]
        d0 = sorted_latencies[int(f)] * (c - k)
        d1 = sorted_latencies[int(c)] * (k - f)
        return d0 + d1

    p50_latency = get_percentile(50)
    p99_latency = get_percentile(99)

    print(f"Total Requests: {len(valid_latencies)}")
    print(f"Average Latency: {avg_latency:.4f} s")
    print(f"P50 Latency: {p50_latency:.4f} s")
    print(f"P99 Latency: {p99_latency:.4f} s")

    # Throughput analysis
    start_times = []
    end_times = []
    
    for req_id, info in ids.items():
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
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        
        file_exists = os.path.isfile(output_csv)
        with open(output_csv, 'a', newline='') as csvfile:
            first_col = 'workflow' if workflow_name else 'File'
            fieldnames = [first_col, 'Total Requests', 'Average Latency', 'P50 Latency', 'P99 Latency', 'Experiment Duration', 'Average Throughput']
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
                'Average Throughput': f"{throughput:.2f}"
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
