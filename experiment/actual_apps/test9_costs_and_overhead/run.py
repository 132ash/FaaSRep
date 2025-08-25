from gevent import monkey
monkey.patch_all()
import gevent
import boto3
import logging
import json
import sys
import time
import pandas as pd
import multiprocessing
import requests

from pathlib import Path
script_dir = Path(__file__).parent
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

import config.config as config
from experiment.common import repository, client_logs
from experiment.common import generate_param


DB_NODE_IP = config.STORAGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# --- 全局测试参数 ---
CLIENT_CNT = 32
ROUND = 100
all_workflows = ['travel_reservation', 'social_network', 'banking_system']

WORKER_NODES = ['10.2.27.23', '10.2.30.50', '10.2.30.62']
AGENT_PORT = 5001

def start_metrics_collection():
    """使用 gevent 并行向监控代理发送请求以启动数据收集进程。"""
    logging.info("Sending requests in parallel to start metrics collection on all worker nodes...")

    def _send_start_request(worker_ip):
        """发送单个启动请求的辅助函数。"""
        try:
            response = requests.post(f'http://{worker_ip}:{AGENT_PORT}/start_metrics', timeout=5)
            if response.status_code == 200:
                logging.info(f"Successfully started metrics collection on {worker_ip}.")
            else:
                logging.warning(f"Failed to start metrics collection on {worker_ip}: {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error contacting metrics agent on {worker_ip} to start: {e}", file=sys.stderr)

    jobs = [gevent.spawn(_send_start_request, worker_ip) for worker_ip in WORKER_NODES]
    gevent.joinall(jobs, timeout=10)

def stop_metrics_collection() -> list:
    """
    使用 gevent 并行向各节点发送停止收集请求，并接收并返回各节点返回的指标数据。
    """
    logging.info("Sending requests in parallel to stop metrics collection and gather results...")

    def _send_stop_request(worker_ip):
        """发送单个停止请求并返回JSON数据的辅助函数。"""
        try:
            response = requests.post(f'http://{worker_ip}:{AGENT_PORT}/stop_metrics', timeout=10)
            if response.status_code == 200:
                metrics = response.json()
                logging.info(f"Successfully stopped metrics on {worker_ip} and received data.")
                return metrics
            else:
                logging.error(f"Failed to stop metrics on {worker_ip}: {response.status_code} {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error contacting metrics agent on {worker_ip} to stop: {e}", file=sys.stderr)
            return None

    jobs = [gevent.spawn(_send_stop_request, worker_ip) for worker_ip in WORKER_NODES]
    gevent.joinall(jobs, timeout=15)
    
    # 从完成的 greenlet 中收集有效的返回结果
    all_metrics_data = [job.value for job in jobs if job.value is not None]
    
    return all_metrics_data

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
    local_results = []
    for i in range(ROUND):
        transaction_id = parameters_all_round[i]['transaction_id']
        # logging.info(f"[{client_id}] Round {i+1}/{ROUND} for workflow {workflow}, txid:{transaction_id}")
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if i % max(1, (ROUND // 10)) == 0:
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} for workflow {workflow}, txid:{transaction_id}")

def workflow_process_task(workflow, workflow_result_queue):
    """每个工作流的主进程任务，负责运行客户端并收集原始结果。"""
    try:
        # 为每个工作流进程设置独立的日志
        logging.basicConfig(
            level=logging.INFO,
            format=f'[{workflow}] %(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(script_dir / f'{workflow}_process.log', mode='w', encoding='utf-8')
            ]
        )
        
        logging.info(f"开始处理工作流: {workflow}")
        
        # 生成参数
        parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, CLIENT_CNT, ROUND)
        result_queue = multiprocessing.Queue()
        
        # 创建客户端子进程
        processes = []
        for i in range(CLIENT_CNT):
            process = multiprocessing.Process(
                target=worker_task, 
                args=(i, workflow, parameters_all[i], result_queue)
            )
            processes.append(process)
        
        # 启动所有客户端进程并计时
        start_time = time.time()
        for i in range(CLIENT_CNT):
            processes[i].start()
            logging.info(f"Started client process {processes[i].pid} for client {i}")
            
        # 等待所有客户端进程结束
        for process in processes:
            process.join()
        end_time = time.time()
        total_time = end_time - start_time

        logging.info(f"工作流 {workflow} 处理完成。")
        
    except Exception as e:
        logging.error(f"工作流 {workflow} 处理出错: {e}")
        import traceback
        traceback.print_exc()
        workflow_result_queue.put((workflow, [], 0)) # 发送空结果以防阻塞

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    transaction_id = parameters.pop('transaction_id', None)
    if transaction_id:
        inputs['transaction_id'] = transaction_id
    rep = requests.post(url, json = inputs)
    return rep.json()

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    transaction_id = rep.get('transaction_id', '')
    return transaction_id, {
        "e2e_latency": rep.get('e2e_latency', 0)
    }, rep['status']

def analyze_all_workflows(system_mode):
    """并行分析所有工作流，并汇总结果"""
    # 清理之前的延迟数据
    repo = repository.Repository()
    repo.flush_couchdb_workflow_latency()
    
    # 创建工作流结果队列
    workflow_result_queue = multiprocessing.Queue()
    
    # 为每个工作流创建一个独立的进程
    workflow_processes = []
    for workflow in all_workflows:
        process = multiprocessing.Process(
            target=workflow_process_task,
            args=(workflow, workflow_result_queue)
        )
        workflow_processes.append(process)
    
    # 启动指标收集
    start_metrics_collection()
    
    # 启动所有工作流进程
    for i, process in enumerate(workflow_processes):
        process.start()
        logging.info(f"Started workflow process {process.pid} for {all_workflows[i]}")
    
    # 等待所有工作流进程完成
    for p in workflow_processes:
        p.join()

    # 停止指标收集并获取结果
    collected_metrics = stop_metrics_collection()

    # --- 新增：处理和保存收集到的指标 ---
    if not collected_metrics:
        logging.warning("No metrics were collected from worker nodes. Skipping metrics summary.")
        return

    # 将所有节点的指标数据转换为DataFrame以便于计算
    # 注意：需要将字符串格式的数值转换为浮点数
    metrics_df = pd.DataFrame(collected_metrics)
    numeric_cols = [
        "duration_seconds", "avg_sink_cpu_cores", "avg_sink_mem_mb",
        "avg_workersp_cpu_cores", "avg_workersp_mem_mb",
        "avg_net_rx_kbps", "avg_net_tx_kbps"
    ]
    for col in numeric_cols:
        metrics_df[col] = pd.to_numeric(metrics_df[col], errors='coerce')

    # 计算所有节点的平均指标
    avg_metrics = metrics_df[numeric_cols].mean().to_dict()
    
    # 创建汇总DataFrame并保存
    summary_df = pd.DataFrame([avg_metrics])
    
    results_dir = script_dir / 'results'
    summary_file = results_dir / f"{system_mode}_system_metrics_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    
    logging.info(f"System metrics summary saved to {summary_file}")
    print("\n--- System Metrics Summary (Averaged Across All Nodes) ---")
    print(summary_df.to_string(index=False))
    print("----------------------------------------------------------")


if __name__ == '__main__':
    # 为主进程设置一个通用的日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format='[MainProcess] %(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    results_dir = script_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    analyze_all_workflows("repair")
