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

# 将 repo 定义为全局变量，但初始为 None
repo = None
def init_repo():
    """为每个进程初始化独立的 repo 实例。"""
    global repo
    if repo is None:
        repo = repository.Repository()

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
    client_logs.setup_logging_for_process(script_dir, client_id)
    local_results = []
    for i in range(ROUND):
        transaction_id = parameters_all_round[i]['transaction_id']
        #logging.info(f"[{client_id}] Starting transaction {i+1}/{ROUND} for workflow {workflow} with transaction ID {transaction_id}.")
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if tx_status == 'aborted':
            continue
        else:
            local_results.append(result)
        if i % (ROUND // 10) == 0 or i == ROUND - 1:
            logging.info(f"[{client_id}] Completed transaction {i+1}/{ROUND}.")
    logging.info(f"[{client_id}] Completed {len(local_results)} valid transactions for workflow {workflow}.")
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
    
    result_details = {
        "transaction_id": txid,
        "e2e_latency": rep.get('e2e_latency', 0),
        "first_run_latency": rep.get('first_run_latency', 0),
        "time_inside_validator": rep.get('time_inside_validator', 0),
        "time_repair": rep.get('time_repair', 0),
        "time_commit": rep.get('time_commit', 0),
    }
    return txid, result_details, tx_status

def write_summary_to_file(system_mode, client_cnt, summary_stats):
    """将只包含平均延迟的汇总结果追加到总的汇总文件中。"""
    summary_file = script_dir / 'results' / "summary_results.csv"
    
    headers = [
        "system_mode", "client_count", "avg_throughput", "avg_e2e_latency",
        "avg_scheduling_latency", "avg_func_exec_latency", "avg_io_latency",
        "avg_time_inside_validator", "avg_time_repair", "avg_time_commit"
    ]
    
    if not summary_file.exists():
        with open(summary_file, 'w') as f:
            f.write(','.join(headers) + '\n')
        logging.info(f"Created summary file: {summary_file}")
        
    with open(summary_file, 'a') as f:
        # 从第4个元素开始格式化
        data_row = [f"{summary_stats.get(header, 0):.4f}" for header in headers[3:]]
        f.write(f"{system_mode},{client_cnt},{summary_stats.get('avg_throughput', 0):.4f}," + ','.join(data_row) + '\n')
    
    logging.info(f"Appended summary for {system_mode} with {client_cnt} clients to {summary_file}")

def analyze_workflow_performance(system_mode, workflow, client_cnt):
    """分析单个工作流的性能，并保存包含所有延迟分量的结果。"""
    logging.info(f"--- Starting analysis for {workflow} in {system_mode} mode with {client_cnt} clients ---")
    
    init_repo() # 在主进程中初始化 repo
    repo.flush_couchdb_workflow_latency()
    
    parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, client_cnt, ROUND)
    result_queue = multiprocessing.Queue()
    
    processes = [multiprocessing.Process(target=worker_task, args=(i, workflow, parameters_all[i], result_queue)) for i in range(client_cnt)]
    
    start_time = time.time()
    for p in processes:
        p.start()

    all_results = []
    for _ in range(client_cnt):
        all_results.extend(result_queue.get())

    for p in processes:
        p.join()
        
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"All processes completed and results collected in {total_time:.2f} seconds.")
        
    if not all_results:
        logging.error(f"No valid results collected for {workflow} in {system_mode} mode.")
        return

    # --- 批量获取IO延迟并后处理 ---
    logging.info(f"所有工作进程已完成。开始批量获取 {len(all_results)} 条记录的IO和EXEC延迟...")
    
    tx_ids = [res['transaction_id'] for res in all_results]
    
    # 使用合并后的函数，通过 phase 参数区分
    io_latencies = repo.get_latencies_for_txs_by_phase(tx_ids, 'io')
    func_e2e_latencies = repo.get_latencies_for_txs_by_phase(tx_ids, 'exec')
    
    # 将延迟合并回结果列表，并计算所有延迟分量
    for res in all_results:
        tx_id = res['transaction_id']
        
        io_latency = io_latencies.get(tx_id, 0)
        func_e2e_latency = func_e2e_latencies.get(tx_id, 0)
        first_run_latency = res.get('first_run_latency', 0)
        
        # 计算新的延迟分量 (移除保护)
        func_exec_latency = func_e2e_latency - io_latency
        scheduling_latency = first_run_latency - func_e2e_latency
        
        # 存储所有分量
        res['io_latency'] = io_latency
        res['func_e2e_latency'] = func_e2e_latency
        res['func_exec_latency'] = func_exec_latency
        res['scheduling_latency'] = scheduling_latency

    logging.info("延迟数据分解完成。")

    df = pd.DataFrame(all_results)
    
    initial_rows = len(df)
    latency_columns_to_clean = ["time_repair", "time_commit", "e2e_latency", "first_run_latency"]
    for col in latency_columns_to_clean:
        df = df[(df[col] >= 0) & (df[col] < 10)]
    
    cleaned_rows = len(df)
    if initial_rows > cleaned_rows:
        logging.warning(f"Filtered out {initial_rows - cleaned_rows} rows due to outlier latency values.")
    
    results_dir = script_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_output_file = results_dir / f"{system_mode}_raw_details.csv"
    df.to_csv(raw_output_file, mode='a', header=not raw_output_file.exists(), index=False)
    logging.info(f"Saved/Appended {cleaned_rows} cleaned raw results for {system_mode} to {raw_output_file}")
    
    summary_stats = {}
    summary_stats['avg_throughput'] = len(df) / total_time if total_time > 0 else 0
    
    latency_columns = [
        "e2e_latency", "scheduling_latency", "func_exec_latency", "io_latency",
        "time_inside_validator", "time_repair", "time_commit"
    ]
    
    print(f"\n--- {system_mode} Mode ({client_cnt} clients) Summary ---")
    print(f"  Average Throughput: {summary_stats['avg_throughput']:.2f} RPS")
    
    for col in latency_columns:
        mean_val = df[col].mean()
        summary_stats[f'avg_{col}'] = mean_val
        print(f"  Average {col:<22}: {mean_val:.4f}s")

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
    
    system_modes_to_test = "OPTIMISTIC"
    
    summary_file_to_clear = results_dir / "summary_results.csv"
    if summary_file_to_clear.exists():
        summary_file_to_clear.unlink()
        logging.info(f"Cleared old summary file: {summary_file_to_clear}")
        
    raw_file_to_clear = results_dir / f"{system_modes_to_test}_raw_details.csv"
    if raw_file_to_clear.exists():
        raw_file_to_clear.unlink()
        logging.info(f"Cleared old raw details file for {system_modes_to_test}: {raw_file_to_clear}")
    analyze_workflow_performance(system_modes_to_test, workflow_to_test, CLIENT_CNT)

    logging.info("\n=== Execution Complete ===")