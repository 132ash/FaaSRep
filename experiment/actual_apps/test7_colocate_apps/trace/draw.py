import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
from pathlib import Path
import math
import argparse

# --- 配置 ---
# 默认结果文件路径
DEFAULT_RESULT_FILE = '/home/shao/FaaSnap/experiment/actual_apps/test7_colocate_apps/trace/result/travel_reservation/travel_merged.json'
DEFAULT_OUTPUT_IMAGE = 'combined_throughput_curve.png'
TRACE_FILE = 'trace_tidy.json'

def load_data(filepath):
    print(f"Loading data from {filepath}...")
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {filepath}")
        return None

def load_trace_data(script_dir, trace_id, start_idx, duration):
    trace_path = script_dir / TRACE_FILE
    # print(f"Loading trace data from {trace_path}...")
    try:
        if not trace_path.exists():
            return None
            
        with open(trace_path, 'r') as f:
            raw_trace = json.load(f)
        
        if 'per_function_invocations' not in raw_trace:
            return None
            
        timestamps = raw_trace['per_function_invocations'][trace_id]['incoming_timestamps'][start_idx:]
        
        if not timestamps:
            return None
            
        start_ts = timestamps[0]
        end_ts = start_ts + duration
        
        valid_timestamps = []
        for ts in timestamps:
            if ts > end_ts:
                break
            valid_timestamps.append(ts)
            
        relative_timestamps = [t - start_ts for t in valid_timestamps]
        return relative_timestamps
    except Exception as e:
        print(f"Error loading trace: {e}")
        return None

def get_throughput_series(data, base_time=None, duration_limit=None):
    ids_data = data.get('ids', {})
    if not ids_data:
        return None, 0, 0, 0

    end_times = []
    start_times = []
    for info in ids_data.values():
        if 'ed' in info and 'st' in info:
            end_times.append(info['ed'])
            start_times.append(info['st'])
            
    if not end_times:
        return None, 0, 0, 0

    if base_time is None:
        base_time = min(start_times)

    relative_times = [math.floor(t - base_time) for t in end_times]

    df = pd.DataFrame({'second': relative_times})
    throughput_series = df['second'].value_counts().sort_index()
    
    throughput_series = throughput_series[throughput_series.index >= 0]
    
    # Remove zero throughput segments (do not reindex with zeros)
    # throughput_series = throughput_series[throughput_series > 0] # value_counts only has >0
    
    duration = throughput_series.index.max() if not throughput_series.empty else 0
    
    return throughput_series, len(end_times), duration, base_time

def plot_combined_throughput(datasets, output_file, trace_data=None):
    plt.figure(figsize=(12, 6))
    
    # Auto-assign colors if not in map
    colors_map = {'Optimistic': 'blue', 'Pessimistic': 'red', 'Travel': 'green', 'Banking': 'orange'}
    default_colors = ['purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
    
    for i, (label, data) in enumerate(datasets.items()):
        if data is None:
            continue
            
        series, total_reqs, duration, _ = get_throughput_series(data, base_time=None, duration_limit=None)
        if series is None:
            print(f"Skipping {label}: No valid data")
            continue
            
        color = colors_map.get(label, default_colors[i % len(default_colors)])
        plt.plot(series.index, series.values, linewidth=1, label=label, color=color)
        
        print(f"[{label}] Total Reqs: {total_reqs}, Duration: {duration:.2f}s, Avg QPS: {total_reqs/duration:.2f}")

    plt.title('System Throughput Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Throughput (Requests/Second)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xlim(left=0)
    
    plt.savefig(output_file)
    print(f"Combined throughput plot saved to {output_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='Path to merged result file', default=DEFAULT_RESULT_FILE)
    parser.add_argument('--label', help='Label for the plot', default='Travel')
    parser.add_argument('--output', help='Output image file', default=DEFAULT_OUTPUT_IMAGE)
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    datasets = {}
    
    file_path = Path(args.file)
    if not file_path.is_absolute():
        file_path = script_dir / file_path
        
    data = load_data(file_path)
    if data:
        datasets[args.label] = data
            
    # Trace data loading (optional/legacy)
    trace_id = 1
    start_idx = 105674
    exp_duration = 3600
    trace_data = load_trace_data(script_dir, trace_id, start_idx, exp_duration)
            
    if datasets:
        output_path = Path(args.output)
        if not output_path.is_absolute():
            output_path = script_dir / output_path
        plot_combined_throughput(datasets, output_path, trace_data)
    else:
        print("No data loaded.")

if __name__ == '__main__':
    main()