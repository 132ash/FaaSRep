import json
import sys
import math
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

# --- 配置 ---
TARGET_IDS = [1]
WINDOW_SIZE = 3600  # 窗口大小：3600秒 (1小时)
TRACE_FILE_NAME = 'trace_tidy.json'

def load_trace(file_path):
    print(f"正在加载 trace 文件: {file_path} ...")
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到文件 {file_path}")
        sys.exit(1)

def get_timestamps_for_id(trace_data, func_id):
    """从 trace 数据中提取指定 ID 的时间戳列表"""
    # 尝试作为字典键 (字符串)
    str_id = str(func_id)
    per_func = trace_data.get('per_function_invocations', {})
    
    if isinstance(per_func, dict):
        if str_id in per_func:
            return per_func[str_id].get('incoming_timestamps', [])
    elif isinstance(per_func, list):
        # 尝试作为列表索引
        if 0 <= func_id < len(per_func):
            return per_func[func_id].get('incoming_timestamps', [])
            
    print(f"警告: 未找到 ID {func_id} 的数据")
    return []

def find_densest_window(timestamps, window_duration):
    """
    找出包含最多请求的持续时间为 window_duration 的区间。
    返回: (max_count, start_time, end_time, start_index_in_timestamps)
    """
    if not timestamps:
        return 0, 0, 0, 0

    n = len(timestamps)
    if n == 0:
        return 0, 0, 0, 0

    # 确保时间戳是排序的
    timestamps.sort()

    max_requests = 0
    best_start_time = timestamps[0]
    best_start_idx = 0
    
    # 滑动窗口算法
    # left 指针指向窗口开始的请求索引
    # right 指针指向窗口结束的请求索引
    left = 0
    for right in range(n):
        # 当前窗口的时间跨度: timestamps[right] - timestamps[left]
        # 如果跨度超过了 window_duration，左指针右移，直到跨度 <= window_duration
        # 注意：题目要求是"连续的3600秒"，即 timestamps[right] - timestamps[left] <= 3600
        while timestamps[right] - timestamps[left] > window_duration:
            left += 1
        
        current_requests = right - left + 1
        
        if current_requests > max_requests:
            max_requests = current_requests
            best_start_time = timestamps[left]
            best_start_idx = left

    return max_requests, best_start_time, best_start_time + window_duration, best_start_idx

def calculate_rps_series(timestamps, start_time, duration):
    """计算指定时间段内的每秒 RPS 序列"""
    end_time = start_time + duration
    # 筛选出该时间段内的所有时间戳
    window_timestamps = [t for t in timestamps if start_time <= t <= end_time]
    
    if not window_timestamps:
        return np.zeros(int(duration))

    # 将时间戳转换为相对于 start_time 的秒数偏移量
    relative_times = [int(t - start_time) for t in window_timestamps]
    
    # 统计每秒的请求数
    # 创建一个长度为 duration 的数组
    rps_series = np.zeros(int(duration) + 1)
    for t in relative_times:
        if 0 <= t < len(rps_series):
            rps_series[t] += 1
            
    return rps_series

def main():
    script_dir = Path(__file__).parent
    trace_path = script_dir / TRACE_FILE_NAME
    
    data = load_trace(trace_path)
    
    results = {}
    
    print(f"\n{'='*60}")
    print(f"分析结果 (窗口大小: {WINDOW_SIZE}秒)")
    print(f"{'='*60}")

    plt.figure(figsize=(15, 8))

    for fid in TARGET_IDS:
        timestamps = get_timestamps_for_id(data, fid)
        if not timestamps:
            continue
            
        # 1. 找出最密集的窗口
        max_req, start_t, end_t, start_idx = find_densest_window(timestamps, WINDOW_SIZE)
        avg_rps = max_req / WINDOW_SIZE
        
        print(f"Function ID: {fid}")
        print(f"  - 最密集时段总请求数: {max_req}")
        print(f"  - 平均 RPS: {avg_rps:.2f}")
        print(f"  - 时间区间: {start_t:.2f}s 到 {end_t:.2f}s")
        print(f"  - 原始 Trace 中的起始索引 (start_idx): {start_idx}")
        print(f"  - 原始 Trace 中的结束索引: {start_idx + max_req - 1}")
        print("-" * 40)
        
        # 2. 准备绘图数据
        rps_series = calculate_rps_series(timestamps, start_t, WINDOW_SIZE)
        
        # 3. 绘图
        time_axis = np.arange(len(rps_series))
        plt.plot(time_axis, rps_series, label=f'Func {fid} (Start T={start_t:.0f}s, Avg RPS={avg_rps:.1f})', alpha=0.7, linewidth=1)

    plt.title(f'RPS During Peak {WINDOW_SIZE}s Window for Each Function')
    plt.xlabel('Time (seconds within the window)')
    plt.ylabel('Requests Per Second (RPS)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    output_img = script_dir / 'peak_hour_rps.png'
    plt.savefig(output_img)
    print(f"\n图表已保存至: {output_img}")
    # plt.show()

if __name__ == "__main__":
    main()