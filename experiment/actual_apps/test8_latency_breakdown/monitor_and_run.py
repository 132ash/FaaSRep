import time
import threading
import subprocess
import psutil
import docker
import sys
from collections import defaultdict

# --- 配置 ---
MONITOR_INTERVAL = 1  # 秒，数据采集频率
NETWORK_INTERFACE = 'eno12399np0' # 需要监控的网卡名称
SINK_SCRIPT_NAME = 'transaction_sink/proxy.py'
WORKERSP_SCRIPT_NAME = 'workflow_manager/proxy.py'
EXPERIMENT_SCRIPT = 'run.py'

# 全局变量，用于存储采集的数据
metrics_data = {
    'sink_cpu': [],
    'sink_mem': [],
    'workersp_cpu': [],
    'workersp_mem': [],
    'container_mem': [],
    'net_rx': [], # 接收带宽 (Bytes/s)
    'net_tx': [], # 发送带宽 (Bytes/s)
}

# 用于停止监控的事件标志
stop_monitoring = threading.Event()

def find_process_by_script(script_name):
    """通过脚本名称查找正在运行的进程。"""
    for p in psutil.process_iter(['pid', 'cmdline']):
        if p.info['cmdline'] and script_name in ' '.join(p.info['cmdline']):
            return p
    return None

def monitor_worker():
    """在后台运行的监控工作线程。"""
    print("监控线程已启动...")
    
    # 初始化 Docker 客户端
    try:
        docker_client = docker.from_env()
        docker_client.ping()
    except Exception as e:
        print(f"错误：无法连接到 Docker. 请确保 Docker 正在运行。 {e}")
        return

    # 初始化网络监控
    last_net_io = psutil.net_io_counters(pernic=True).get(NETWORK_INTERFACE)
    if not last_net_io:
        print(f"错误：找不到网络接口 '{NETWORK_INTERFACE}'.")
        return
    last_time = time.time()

    # 初始化进程监控
    sink_process = find_process_by_script(SINK_SCRIPT_NAME)
    workersp_process = find_process_by_script(WORKERSP_SCRIPT_NAME)

    if not sink_process:
        print(f"警告：未找到 Sink 进程 ({SINK_SCRIPT_NAME}).")
    if not workersp_process:
        print(f"警告：未找到 WorkerSP 进程 ({WORKERSP_SCRIPT_NAME}).")

    # 初始化CPU监控（第一次调用返回0，用于启动计时器）
    if sink_process: sink_process.cpu_percent(interval=None)
    if workersp_process: workersp_process.cpu_percent(interval=None)
    
    while not stop_monitoring.is_set():
        # --- 1. 监控进程 ---
        if sink_process and sink_process.is_running():
            metrics_data['sink_cpu'].append(sink_process.cpu_percent(interval=None))
            metrics_data['sink_mem'].append(sink_process.memory_info().rss / (1024 * 1024)) # MB
        
        if workersp_process and workersp_process.is_running():
            metrics_data['workersp_cpu'].append(workersp_process.cpu_percent(interval=None))
            metrics_data['workersp_mem'].append(workersp_process.memory_info().rss / (1024 * 1024)) # MB

        # --- 2. 监控容器 ---
        try:
            containers = docker_client.containers.list(filters={'label': 'workflow'})
            total_container_mem = 0
            if containers:
                for container in containers:
                    stats = container.stats(stream=False)
                    total_container_mem += stats.get('memory_stats', {}).get('usage', 0)
                avg_container_mem = (total_container_mem / len(containers)) / (1024 * 1024) # MB
                metrics_data['container_mem'].append(avg_container_mem)
        except Exception as e:
            print(f"\n警告：无法获取容器统计信息: {e}")


        # --- 3. 监控网络 ---
        current_net_io = psutil.net_io_counters(pernic=True).get(NETWORK_INTERFACE)
        current_time = time.time()
        
        elapsed_time = current_time - last_time
        if elapsed_time > 0 and current_net_io:
            rx_speed = (current_net_io.bytes_recv - last_net_io.bytes_recv) / elapsed_time
            tx_speed = (current_net_io.bytes_sent - last_net_io.bytes_sent) / elapsed_time
            metrics_data['net_rx'].append(rx_speed)
            metrics_data['net_tx'].append(tx_speed)
        
        last_net_io = current_net_io
        last_time = current_time

        time.sleep(MONITOR_INTERVAL)
    
    print("监控线程已停止。")

def calculate_and_print_results():
    """计算并打印所有指标的平均值。"""
    print("\n" + "="*50)
    print("实验监控结果汇总")
    print("="*50)

    def avg(data):
        return sum(data) / len(data) if data else 0

    # 进程指标
    print("\n--- 进程资源占用 (平均值) ---")
    print(f"Sink CPU 占用        : {avg(metrics_data['sink_cpu']):.2f} %")
    print(f"Sink 内存占用        : {avg(metrics_data['sink_mem']):.2f} MB")
    print(f"WorkerSP CPU 占用    : {avg(metrics_data['workersp_cpu']):.2f} %")
    print(f"WorkerSP 内存占用    : {avg(metrics_data['workersp_mem']):.2f} MB")

    # 容器指标
    print("\n--- 容器资源占用 (平均值) ---")
    print(f"Workflow容器内存占用 : {avg(metrics_data['container_mem']):.2f} MB")

    # 网络指标
    avg_rx_mbps = (avg(metrics_data['net_rx']) * 8) / (1024 * 1024)
    avg_tx_mbps = (avg(metrics_data['net_tx']) * 8) / (1024 * 1024)
    print(f"\n--- 网络带宽占用 (平均值, {NETWORK_INTERFACE}) ---")
    print(f"接收带宽 (RX)        : {avg_rx_mbps:.2f} Mbps")
    print(f"发送带宽 (TX)        : {avg_tx_mbps:.2f} Mbps")
    
    print("\n" + "="*50)


if __name__ == '__main__':
    # 确保目标进程已经启动
    print("等待目标服务进程启动...")
    while not find_process_by_script(SINK_SCRIPT_NAME) or not find_process_by_script(WORKERSP_SCRIPT_NAME):
        print("  - 仍在等待 sink 和 workersp 服务启动，请确保它们正在运行...")
        time.sleep(2)
    print("服务进程已找到。")

    # 启动监控线程
    monitor_thread = threading.Thread(target=monitor_worker)
    monitor_thread.start()
    
    # 给予监控线程一点时间来初始化
    time.sleep(1)

    # 运行实验脚本
    print(f"\n>>> 开始运行实验脚本: {EXPERIMENT_SCRIPT}...")
    try:
        # 使用 Popen 启动子进程，并捕获其输出
        experiment_process = subprocess.Popen(
            ['python3', EXPERIMENT_SCRIPT],
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        experiment_process.wait() # 等待实验脚本执行完毕
    except FileNotFoundError:
        print(f"错误: 找不到实验脚本 '{EXPERIMENT_SCRIPT}'。")
    except Exception as e:
        print(f"实验脚本运行时发生错误: {e}")
    finally:
        print(f">>> 实验脚本 {EXPERIMENT_SCRIPT} 已结束。\n")
        
        # 停止监控
        stop_monitoring.set()
        monitor_thread.join()
        
        # 计算并打印结果
        calculate_and_print_results()


### 如何使用

# 1.  **启动您的服务**: 像往常一样，在不同的终端中启动 `transaction_sink/proxy.py` 和 `workflow_manager/proxy.py`。
# 2.  **运行监控脚本**: 在 `test9_latency_breakdown` 目录下，运行新创建的脚本：
#     ```bash
#     python3 monitor_and_run.py
#     ```
# 3.  **观察输出**:
#     *   脚本会首先确认 `sink` 和 `workersp` 进程已经找到。
#     *   然后它会启动 `run.py`，您会看到 `run.py` 的正常输出。
#     *   当 `run.py` 运行结束后，脚本会自动停止监控，并打印出所有系统指标的平均值汇总报告。

# 这个脚本为您提供了一个强大而灵活的框架，用于在不修改核心代码的情况下，对任何实验进行外部性能监控。# filepath: /home/shao/FaaSnap/experiment/actual_apps/test9_latency_breakdown/monitor_and_run.py
