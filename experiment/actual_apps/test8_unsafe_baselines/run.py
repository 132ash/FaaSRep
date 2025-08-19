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
CLIENT_CNT = 16
ROUND = 100
# 只测试 travel_reservation
workflow_to_test = 'travel_reservation'

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
    client_logs.setup_logging_for_process(script_dir, client_id)
    local_results = []
    for i in range(ROUND):
        transaction_id = parameters_all_round[i]['transaction_id']
        logging.info(f"[{client_id}] Round {i+1}/{ROUND} for workflow {workflow}, txid:{transaction_id}")
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if tx_status == 'aborted':
            continue
        else:
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} completed for workflow {workflow}, txid: {txid}, result: {result}")
            local_results.append(result)
    result_queue.put(local_results)

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

def write_summary_to_file(system_mode, client_cnt, p50_latency, p99_latency, avg_throughput):
    """将单次运行的汇总结果追加到总的汇总文件中。"""
    summary_file = script_dir / 'results' / "summary_results.csv"
    
    # 如果文件不存在，则创建并写入表头
    if not summary_file.exists():
        with open(summary_file, 'w') as f:
            f.write("system_mode,client_count,p50_e2e_latency,p99_e2e_latency,avg_throughput\n")
        logging.info(f"Created summary file: {summary_file}")
        
    # 以追加模式写入当前运行的结果
    with open(summary_file, 'a') as f:
        f.write(f"{system_mode},{client_cnt},{p50_latency:.4f},{p99_latency:.4f},{avg_throughput:.4f}\n")
    
    logging.info(f"Appended summary for {system_mode} with {client_cnt} clients to {summary_file}")

def analyze_workflow_performance(system_mode, workflow, client_cnt):
    """分析单个工作流的性能，并保存结果。"""
    logging.info(f"--- Starting analysis for {workflow} in {system_mode} mode with {client_cnt} clients ---")
    
    # 清理之前的延迟数据
    repo = repository.Repository()
    repo.flush_couchdb_workflow_latency()
    
    # 生成参数
    parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, client_cnt, ROUND)
    result_queue = multiprocessing.Queue()
    
    # 创建并启动客户端进程
    processes = [multiprocessing.Process(target=worker_task, args=(i, workflow, parameters_all[i], result_queue)) for i in range(client_cnt)]
    
    start_time = time.time()
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    end_time = time.time()
    total_time = end_time - start_time
    
    # 收集结果
    all_results = []
    while not result_queue.empty():
        all_results.extend(result_queue.get())
        
    if not all_results:
        logging.error(f"No valid results collected for {workflow} in {system_mode} mode.")
        return

    # --- 结果处理 ---
    df = pd.DataFrame(all_results)
    
    # 1. 存储原始延迟数据
    raw_details_dir = script_dir / 'results' / 'raw_details'
    raw_details_dir.mkdir(parents=True, exist_ok=True)
    raw_output_file = raw_details_dir / f"{system_mode}_{workflow}_{client_cnt}clients_raw.csv"
    df.to_csv(raw_output_file, index=False)
    logging.info(f"Saved raw results to {raw_output_file}")
    
    # 2. 计算核心指标
    p50_latency = df['e2e_latency'].quantile(0.50)
    p99_latency = df['e2e_latency'].quantile(0.99)
    avg_throughput = len(df) / total_time if total_time > 0 else 0
    
    # 3. 将总结数据追加到汇总文件
    write_summary_to_file(system_mode, client_cnt, p50_latency, p99_latency, avg_throughput)
    
    print(f"\n--- {system_mode} Mode ({client_cnt} clients) Summary ---")
    print(f"  P50 E2E Latency: {p50_latency:.4f}s")
    print(f"  P99 E2E Latency: {p99_latency:.4f}s")
    print(f"  Average Throughput: {avg_throughput:.2f} RPS")
    print("--------------------------------------------------\n")


if __name__ == '__main__':
    # 为主进程设置一个通用的日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format='[MainProcess] %(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    # 确保结果目录存在
    (script_dir / 'results').mkdir(exist_ok=True)
    
    # 定义要测试的系统模式
    system_modes_to_test = ["PESSIMISTIC", "OPTIMISTIC"]
    
    # 循环测试每一种系统模式
    for mode in system_modes_to_test:
        analyze_workflow_performance(mode, workflow_to_test, CLIENT_CNT)

    logging.info("\n=== Execution Complete ===")