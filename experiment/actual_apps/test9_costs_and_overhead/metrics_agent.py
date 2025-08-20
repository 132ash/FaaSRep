from gevent import monkey
monkey.patch_all()
import sys
import time
import multiprocessing as mp
import os
from pathlib import Path
from flask import Flask, jsonify, request
import psutil
import docker
import logging
from gevent.pywsgi import WSGIServer

# --- 配置 ---
# 请根据实际情况配置您的网络接口名称
NETWORK_INTERFACE = 'eno8303' 
SINK_PORT = 6000
WORKERSP_PORT = 7500

script_dir = Path(__file__).parent
metrics_dir = script_dir / 'metrics'
metrics_dir.mkdir(exist_ok=True)

app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO)

def collect_system_metrics():
    """子进程：采集系统和网络指标。"""
    system_metrics_file = metrics_dir / 'system_metrics.csv'
    
    # 进程启动时，清空文件并写入表头
    try:
        with open(system_metrics_file, 'w') as f:
            f.write("sink_cpu_cores,sink_mem_mb,workersp_cpu_cores,workersp_mem_mb,net_rx_bytes_per_sec,net_tx_bytes_per_sec\n")
    except IOError as e:
        print(f"无法写入系统指标文件: {e}", file=sys.stderr)
        return

    # 初始化CPU和网络监控
    for p in psutil.process_iter(['pid']):
        try: p.cpu_percent()
        except (psutil.Error, psutil.NoSuchProcess): pass
    
    last_net_io = psutil.net_io_counters(pernic=True).get(NETWORK_INTERFACE)
    last_time = time.time()

    processes = {'sink': None, 'workersp': None}
    for conn in psutil.net_connections(kind='inet'):
        if conn.status == 'LISTEN' and conn.pid is not None:
            if conn.laddr.port == SINK_PORT:
                processes['sink'] = psutil.Process(conn.pid)
            elif conn.laddr.port == WORKERSP_PORT:
                processes['workersp'] = psutil.Process(conn.pid)
            
    
    while True:
        # 在循环内部查找进程，以应对服务重启

        current_metrics = {
            'sink_cpu': 0, 'sink_mem': 0, 'workersp_cpu': 0, 'workersp_mem': 0,
            'net_tx_bps': 0, 'net_rx_bps': 0
        }

        if processes['sink'] and processes['sink'].is_running():
            current_metrics['sink_cpu'] = processes['sink'].cpu_percent() / 100.0
            current_metrics['sink_mem'] = processes['sink'].memory_info().rss / (1024 * 1024)  # MB
        if processes['workersp'] and processes['workersp'].is_running():
            current_metrics['workersp_cpu'] = processes['workersp'].cpu_percent() / 100.0
            current_metrics['workersp_mem'] = processes['workersp'].memory_info().rss / (1024 * 1024)  # MB

        current_net_io = psutil.net_io_counters(pernic=True).get(NETWORK_INTERFACE)
        current_time = time.time()
        elapsed_time = current_time - last_time
        
        # 只有在时间间隔足够大时才进行计算，避免无效的0值
        if current_net_io and last_net_io and elapsed_time > 0.1:
            rx_diff = current_net_io.bytes_recv - last_net_io.bytes_recv
            tx_diff = current_net_io.bytes_sent - last_net_io.bytes_sent
            
            current_metrics['net_rx_bps'] = rx_diff / elapsed_time
            current_metrics['net_tx_bps'] = tx_diff / elapsed_time
            
            # 只有在成功计算后才更新基准
            last_net_io = current_net_io
            last_time = current_time
            
            # 只有在成功计算后才写入文件
            with open(system_metrics_file, 'a') as f:
                f.write(f"{current_metrics['sink_cpu']:.4f},{current_metrics['sink_mem']:.2f},"
                        f"{current_metrics['workersp_cpu']:.4f},{current_metrics['workersp_mem']:.2f},"
                        f"{current_metrics['net_rx_bps']:.2f},{current_metrics['net_tx_bps']:.2f}\n")
        
        time.sleep(0.5)


# --- 关键修改：在全局范围只定义一个引用，而不是对象 ---
system_metrics_process = None
process_lock = mp.Lock() # 创建一个锁
  
@app.route('/start_metrics', methods=['POST'])
def start_metrics():
    global system_metrics_process
    
    with process_lock: # 获取锁
        if system_metrics_process and system_metrics_process.is_alive():
            return jsonify({"message": "Metrics collection is already running."}), 409

        print("收到启动指标采集的请求...")
        system_metrics_process = mp.Process(target=collect_system_metrics, daemon=True)
        system_metrics_process.start()
        
        return jsonify({"message": "Metrics collection started."}), 200
    # 锁在这里自动释放

@app.route('/stop_metrics', methods=['POST'])
def stop_metrics():
    global system_metrics_process
    
    with process_lock: # 获取锁
        print("收到停止指标采集的请求...")
        
        if system_metrics_process and system_metrics_process.is_alive():
            system_metrics_process.terminate()
            system_metrics_process.join(timeout=1) # 等待最多1秒，避免无限阻塞
            system_metrics_process = None # 将引用设为None
            return jsonify({"message": "Metrics collection stopped."}), 200
        else:
            # 如果进程不存在或已停止，也重置一下引用以确保状态一致
            system_metrics_process = None
            return jsonify({"message": "Metrics collection is not running or already stopped."}), 404
    # 锁在这里自动释放
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python metrics_agent.py <ip_address> <port>")
        sys.exit(1)
        
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    print(f"启动监控代理服务器于 http://{host}:{port}")
    server = WSGIServer((host, port), app)
    server.serve_forever()