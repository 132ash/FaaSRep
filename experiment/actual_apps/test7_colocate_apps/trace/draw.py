import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
from pathlib import Path
import math

# --- 配置 ---
# 结果文件的相对路径
RESULT_FILES = {
    'Optimistic': 'result/OPTIMISTIC_Trace_travel_reservation_1_105674.json',
    'Pessimistic': 'result/PESSIMISTIC_Trace_travel_reservation_1_105674.json'
}
# 输出图片的名称
OUTPUT_IMAGE = 'combined_throughput_curve.png'
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
    print(f"Loading trace data from {trace_path}...")
    try:
        with open(trace_path, 'r') as f:
            raw_trace = json.load(f)
        
        # 根据 run.py 的逻辑提取时间戳
        # 注意：raw_trace['per_function_invocations'] 是一个列表
        timestamps = raw_trace['per_function_invocations'][trace_id]['incoming_timestamps'][start_idx:]
        
        # 截取 duration 内的数据
        if not timestamps:
            return None
            
        start_ts = timestamps[0]
        # run.py 逻辑: last_timestamp = incoming_timestamps[0] + exp_duration
        # 这里我们稍微放宽一点，确保覆盖
        end_ts = start_ts + duration
        
        valid_timestamps = []
        for ts in timestamps:
            if ts > end_ts:
                break
            valid_timestamps.append(ts)
            
        # 归一化到 0 开始
        relative_timestamps = [t - start_ts for t in valid_timestamps]
        return relative_timestamps
    except Exception as e:
        print(f"Error loading trace: {e}")
        return None

def get_throughput_series(data, base_time=None, duration_limit=3600):
    # 1. 提取所有请求的完成时间 (end time) 和开始时间 (start time)
    ids_data = data.get('ids', {})
    if not ids_data:
        return None, 0, 0, 0

    # 提取 'ed' (end time) 和 'st' (start time)
    end_times = []
    start_times = []
    for info in ids_data.values():
        if 'ed' in info and 'st' in info:
            end_times.append(info['ed'])
            start_times.append(info['st'])
            
    if not end_times:
        return None, 0, 0, 0

    # 确定基准时间
    # 如果没有提供 base_time，则使用该数据集最早的开始时间
    if base_time is None:
        base_time = min(start_times)

    # 将绝对时间戳转换为相对秒数 (向下取整)
    # 使用 base_time 对齐
    relative_times = [math.floor(t - base_time) for t in end_times]

    # 3. 统计每秒的 QPS
    df = pd.DataFrame({'second': relative_times})
    throughput_series = df['second'].value_counts().sort_index()
    
    # 填补没有请求的秒数
    # 过滤掉负数
    throughput_series = throughput_series[throughput_series.index >= 0]
    
    # 强制对齐到 duration_limit (3600s)
    full_index = range(duration_limit + 1)
    throughput_series = throughput_series.reindex(full_index, fill_value=0)
    
    return throughput_series, len(end_times), duration_limit, base_time

def get_rps_series(timestamps, duration_limit=3600):
    # 统计 Trace 的每秒请求数
    # timestamps 已经是相对时间 (0, 0.1, 0.5, 1.2 ...)
    seconds = [math.floor(t) for t in timestamps]
    df = pd.DataFrame({'second': seconds})
    rps_series = df['second'].value_counts().sort_index()
    
    # 强制对齐到 duration_limit (3600s)
    full_index = range(duration_limit + 1)
    rps_series = rps_series.reindex(full_index, fill_value=0)
    return rps_series

def plot_combined_throughput(datasets, trace_data=None):
    plt.figure(figsize=(12, 6))
    
    colors = {'Optimistic': 'blue', 'Pessimistic': 'red'}
    
    # 1. 绘制 Trace RPS (如果存在)
    if trace_data:
        trace_series = get_rps_series(trace_data)
        plt.plot(trace_series.index, trace_series.values, 
                 color='green', linestyle='--', linewidth=1, alpha=0.7, label='Trace Input (RPS)')
        print(f"[Trace] Total Reqs: {len(trace_data)}, Duration: {trace_series.index.max()}s")

    # 2. 移除全局对齐逻辑，改为各自独立对齐到 0
    # 这样即使实验是在不同时间运行的，也能在同一张图上对比
    
    for label, data in datasets.items():
        if data is None:
            continue
            
        # base_time=None 会自动使用该数据集最早的开始时间作为 0
        # 强制 duration_limit=3600
        series, total_reqs, duration, _ = get_throughput_series(data, base_time=None, duration_limit=3600)
        if series is None:
            print(f"Skipping {label}: No valid data")
            continue
            
        # 绘制原始吞吐量
        plt.plot(series.index, series.values, linewidth=1, label=label, color=colors.get(label, 'gray'))
        
        print(f"[{label}] Total Reqs: {total_reqs}, Duration: {duration:.2f}s, Avg QPS: {total_reqs/duration:.2f}")

    plt.title('System Throughput Comparison: Optimistic vs Pessimistic')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Throughput (Requests/Second)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 设置 X 轴范围为 0 到 3600
    plt.xlim(0, 3600)
    
    # 保存图片
    script_dir = Path(__file__).parent
    output_path = script_dir / OUTPUT_IMAGE
    plt.savefig(output_path)
    print(f"Combined throughput plot saved to {output_path}")

def main():
    script_dir = Path(__file__).parent
    datasets = {}
    
    # 用于加载 Trace 的元数据
    # 强制使用指定的 Trace 参数
    trace_id = 1
    start_idx = 105674
    exp_duration = 3600
    
    for label, filename in RESULT_FILES.items():
        file_path = script_dir / filename
        data = load_data(file_path)
        if data:
            datasets[label] = data
            # 移除从结果文件更新 Trace 元数据的逻辑，以强制使用指定参数
            # if 'trace_id' in data:
            #     trace_id = data['trace_id']
            # if 'start_idx' in data:
            #     start_idx = data['start_idx']
            # if 'exp_duration' in data:
            #     exp_duration = data['exp_duration']
            
    # 加载 Trace 数据
    trace_data = load_trace_data(script_dir, trace_id, start_idx, exp_duration)
            
    if datasets:
        plot_combined_throughput(datasets, trace_data)
    else:
        print("No data loaded.")

if __name__ == '__main__':
    main()