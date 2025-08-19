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
from experiment.common import repository
from experiment.common import generate_param

DB_NODE_IP = config.STORAGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# --- 全局测试参数 ---
CLIENT_CNT = 16
ROUND = 100
# 只测试 travel_reservation
workflow_to_test = 'travel_reservation'

# 在进程启动时初始化 repo
repo = repository.Repository()

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
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
    """运行工作流并解析所有延迟分量"""
    rep = run_workflow(workflow, parameters_input)
    tx_status = rep.get('status')
    
    if tx_status == 'aborted':
        return None, None, tx_status

    txid = rep.get('transaction_id', '')
    first_run_latency = rep.get('first_run_latency', 0)
    
    # 使用 repository 获取该事务的IO延迟总和
    io_latency = repo.get_io_latency_for_tx(txid)
    exec_latency = first_run_latency - io_latency
    result_details = {
        "transaction_id": txid,
        "e2e_latency": rep.get('e2e_latency', 0),
        "first_run_latency": first_run_latency,
        "time_inside_validator": rep.get('time_inside_validator', 0),
        "time_repair": rep.get('time_repair', 0),
        "time_commit": rep.get('time_commit', 0),
        "io_latency": io_latency,
        "exec_latency": exec_latency
    }
    return txid, result_details, tx_status

def write_summary_to_file(system_mode, client_cnt, summary_stats):
    """将只包含平均延迟的汇总结果追加到总的汇总文件中。"""
    summary_file = script_dir / 'results' / "summary_results.csv"
    
    headers = [
        "system_mode", "client_count", "avg_throughput",
        "avg_e2e_latency", "avg_first_run_latency", "avg_exec_latency",
        "avg_io_latency", "avg_time_inside_validator", "avg_time_repair", "avg_time_commit"
    ]
    
    if not summary_file.exists():
        with open(summary_file, 'w') as f:
            f.write(','.join(headers) + '\n')
        logging.info(f"Created summary file: {summary_file}")
        
    with open(summary_file, 'a') as f:
        data_row = [f"{summary_stats.get(header, 0):.4f}" for header in headers[3:]]
        f.write(f"{system_mode},{client_cnt},{summary_stats.get('avg_throughput', 0):.4f}," + ','.join(data_row) + '\n')
    
    logging.info(f"Appended summary for {system_mode} with {client_cnt} clients to {summary_file}")

def analyze_workflow_performance(system_mode, workflow, client_cnt):
    """分析单个工作流的性能，并保存包含所有延迟分量的结果。"""
    logging.info(f"--- Starting analysis for {workflow} in {system_mode} mode with {client_cnt} clients ---")
    
    repo.flush_couchdb_workflow_latency()
    
    parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, client_cnt, ROUND)
    result_queue = multiprocessing.Queue()
    
    processes = [multiprocessing.Process(target=worker_task, args=(i, workflow, parameters_all[i], result_queue)) for i in range(client_cnt)]
    
    start_time = time.time()
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    end_time = time.time()
    total_time = end_time - start_time
    
    all_results = []
    while not result_queue.empty():
        all_results.extend(result_queue.get())
        
    if not all_results:
        logging.error(f"No valid results collected for {workflow} in {system_mode} mode.")
        return

    df = pd.DataFrame(all_results)
    
    # --- 存储所有原始数据到特定于模式的文件 ---
    results_dir = script_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_output_file = results_dir / f"{system_mode}_raw_details.csv"
    # 如果文件已存在，则追加；否则创建新文件
    df.to_csv(raw_output_file, mode='a', header=not raw_output_file.exists(), index=False)
    logging.info(f"Saved/Appended raw results for {system_mode} to {raw_output_file}")
    
    # --- 计算所有核心指标的平均值 ---
    summary_stats = {}
    summary_stats['avg_throughput'] = len(df) / total_time if total_time > 0 else 0
    
    latency_columns = [
        "e2e_latency", "first_run_latency", "exec_latency", "io_latency",
        "time_inside_validator", "time_repair", "time_commit"
    ]
    
    print(f"\n--- {system_mode} Mode ({client_cnt} clients) Summary ---")
    print(f"  Average Throughput: {summary_stats['avg_throughput']:.2f} RPS")
    
    for col in latency_columns:
        mean_val = df[col].mean()
        summary_stats[f'avg_{col}'] = mean_val
        print(f"  Average {col:<20}: {mean_val:.4f}s")

    write_summary_to_file(system_mode, client_cnt, summary_stats)
    print("--------------------------------------------------\n")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[MainProcess] %(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    results_dir = script_dir / 'results'
    results_dir.mkdir(exist_ok=True)
    
    system_modes_to_test = ["PESSIMISTIC", "OPTIMISTIC"]
    
    # 在开始新的一轮测试前，可以选择性地清空旧的汇总文件
    summary_file_to_clear = results_dir / "summary_results.csv"
    if summary_file_to_clear.exists():
        summary_file_to_clear.unlink()
        logging.info(f"Cleared old summary file: {summary_file_to_clear}")

    for mode in system_modes_to_test:
        # 清理特定模式的原始数据文件
        raw_file_to_clear = results_dir / f"{mode}_raw_details.csv"
        if raw_file_to_clear.exists():
            raw_file_to_clear.unlink()
            logging.info(f"Cleared old raw details file for {mode}: {raw_file_to_clear}")
            
        analyze_workflow_performance(mode, workflow_to_test, CLIENT_CNT)

    logging.info("\n=== Execution Complete ===")