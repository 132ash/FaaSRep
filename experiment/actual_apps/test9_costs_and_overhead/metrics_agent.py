from gevent import monkey
monkey.patch_all()
import sys
import time
import multiprocessing as mp
import os
from pathlib import Path
from flask import Flask, jsonify, request
import psutil
import pandas as pd  # 引入 pandas 用于数据分析
import logging
from gevent.pywsgi import WSGIServer

# --- 配置 ---
NETWORK_INTERFACE = 'eno8303' 
SINK_PORT = 6000
WORKERSP_PORT = 7500

script_dir = Path(__file__).parent
metrics_dir = script_dir / 'metrics'
metrics_dir.mkdir(exist_ok=True)
system_metrics_file = metrics_dir / 'system_metrics.csv'

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO)

# --- 指标采集进程 ---
def collect_system_metrics():
    """子进程：采集系统和网络指标。"""
    
    # 初始化CPU监控
    for p in psutil.process_iter(['pid']):
        try: p.cpu_percent()
        except (psutil.Error, psutil.NoSuchProcess): pass
    
    # 查找目标进程
    processes = {'sink': None, 'workersp': None}
    for conn in psutil.net_connections(kind='inet'):
        if conn.status == 'LISTEN' and conn.pid is not None:
            if conn.laddr.port == SINK_PORT:
                processes['sink'] = psutil.Process(conn.pid)
            elif conn.laddr.port == WORKERSP_PORT:
                processes['workersp'] = psutil.Process(conn.pid)

    while True:
        current_metrics = {
            'timestamp': time.time(),
            'sink_cpu': 0, 'sink_mem': 0, 
            'workersp_cpu': 0, 'workersp_mem': 0,
            'net_rx_bytes': 0, 'net_tx_bytes': 0
        }

        # 获取 CPU 和内存
        if processes['sink'] and processes['sink'].is_running():
            current_metrics['sink_cpu'] = processes['sink'].cpu_percent() / 100.0
            current_metrics['sink_mem'] = processes['sink'].memory_info().rss / (1024 * 1024)
        if processes['workersp'] and processes['workersp'].is_running():
            current_metrics['workersp_cpu'] = processes['workersp'].cpu_percent() / 100.0
            current_metrics['workersp_mem'] = processes['workersp'].memory_info().rss / (1024 * 1024)

        # 获取网络字节数（原始值）
        net_io = psutil.net_io_counters(pernic=True).get(NETWORK_INTERFACE)
        if net_io:
            current_metrics['net_rx_bytes'] = net_io.bytes_recv
            current_metrics['net_tx_bytes'] = net_io.bytes_sent

        # 追加到文件
        try:
            with open(system_metrics_file, 'a') as f:
                f.write(f"{current_metrics['timestamp']:.6f},"
                        f"{current_metrics['sink_cpu']:.4f},{current_metrics['sink_mem']:.2f},"
                        f"{current_metrics['workersp_cpu']:.4f},{current_metrics['workersp_mem']:.2f},"
                        f"{current_metrics['net_rx_bytes']},{current_metrics['net_tx_bytes']}\n")
        except IOError as e:
            print(f"写入指标文件时出错: {e}", file=sys.stderr)

        time.sleep(0.33)

# --- Flask 路由 ---
system_metrics_process = None
process_lock = mp.Lock()

@app.route('/start_metrics', methods=['POST'])
def start_metrics():
    global system_metrics_process
    
    with process_lock:
        if system_metrics_process and system_metrics_process.is_alive():
            return jsonify({"message": "指标采集已在运行。"}), 409

        print("收到启动指标采集的请求...")
        
        # 清空文件，写入开始时间戳和表头
        try:
            with open(system_metrics_file, 'w') as f:
                f.write(f"# METRICS COLLECTION STARTED AT: {time.time():.6f}\n")
                f.write("timestamp,sink_cpu_cores,sink_mem_mb,workersp_cpu_cores,workersp_mem_mb,net_rx_bytes,net_tx_bytes\n")
        except IOError as e:
            print(f"无法写入系统指标文件: {e}", file=sys.stderr)
            return jsonify({"error": f"无法写入文件: {e}"}), 500

        system_metrics_process = mp.Process(target=collect_system_metrics, daemon=True)
        system_metrics_process.start()
        
        return jsonify({"message": "指标采集已启动。"}), 200

@app.route('/stop_metrics', methods=['POST'])
def stop_metrics():
    global system_metrics_process
    
    with process_lock:
        if not system_metrics_process or not system_metrics_process.is_alive():
            return jsonify({"message": "指标采集未运行。"}), 400

        print("收到停止指标采集的请求...")
        
        # 关键修改：获取当前时间戳，用于精确分割数据
        stop_timestamp = time.time()
        
        # 写入停止日志
        try:
            with open(system_metrics_file, 'a') as f:
                f.write(f"# METRICS COLLECTION STOPPED AT: {stop_timestamp:.6f}\n")
        except IOError as e:
            print(f"无法向指标文件追加停止日志: {e}", file=sys.stderr)

        # --- 分析采集到的数据 ---
        try:
            # 读取CSV数据，忽略以'#'开头的注释行
            df = pd.read_csv(system_metrics_file, comment='#')
            
            # 关键修改：只使用在停止时间戳之前记录的数据进行分析
            df = df[df['timestamp'] < stop_timestamp]

            if df.empty:
                return jsonify({"message": "指标采集已停止。在指定时间窗口内无数据可分析。"}), 200
            
            # 计算平均 CPU 和内存
            avg_sink_cpu = df['sink_cpu_cores'].mean()
            avg_sink_mem = df['sink_mem_mb'].mean()
            avg_workersp_cpu = df['workersp_cpu_cores'].mean()
            avg_workersp_mem = df['workersp_mem_mb'].mean()

            # 计算平均带宽
            first_row = df.iloc[0]
            last_row = df.iloc[-1]
            
            duration = last_row['timestamp'] - first_row['timestamp']
            total_rx = last_row['net_rx_bytes'] - first_row['net_rx_bytes']
            total_tx = last_row['net_tx_bytes'] - first_row['net_tx_bytes']
            
            # 避免因时长过短而导致除零错误
            if duration > 0.001:
                avg_rx_kbps = (total_rx / duration) / 1024
                avg_tx_kbps = (total_tx / duration) / 1024
            else:
                avg_rx_kbps = 0
                avg_tx_kbps = 0

            # 准备结果
            results = {
                "message": "完成分析。",
                "duration_seconds": f"{duration:.2f}",
                "avg_sink_cpu_cores": f"{avg_sink_cpu:.4f}",
                "avg_sink_mem_mb": f"{avg_sink_mem:.2f}",
                "avg_workersp_cpu_cores": f"{avg_workersp_cpu:.4f}",
                "avg_workersp_mem_mb": f"{avg_workersp_mem:.2f}",
                "avg_net_rx_kbps": f"{avg_rx_kbps:.2f}",
                "avg_net_tx_kbps": f"{avg_tx_kbps:.2f}"
            }
            return jsonify(results), 200

        except Exception as e:
            print(f"指标分析过程中出错: {e}", file=sys.stderr)
            return jsonify({"error": f"指标分析失败: {e}"}), 500

# python3 metrics_agent.py  10.2.30.50 5001
# python3 metrics_agent.py  10.2.27.23 5001
# python3 metrics_agent.py  10.2.30.62 5001
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python metrics_agent.py <ip_address> <port>")
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    print(f"启动监控代理服务器于 http://{host}:{port}")
    server = WSGIServer((host, port), app)
    server.serve_forever()