import boto3
import logging
import json
import sys
import time
import pandas as pd
import multiprocessing
import requests
from pathlib import Path
from collections import defaultdict

# --- 项目路径设置 ---
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
from experiment.common import repository, client_logs, generate_param

# --- 实验配置 ---
CLIENT_CNT = 32
ROUND_PER_CLIENT = 100
TARGET_WORKFLOW = 'travel_reservation'

# --- 初始化 ---
repo = repository.Repository()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_workflow_request(workflow_name, parameters):
    """向网关发送运行工作流的请求。"""
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow': workflow_name, 'parameters': parameters}
    try:
        rep = requests.post(url, json=inputs)
        return rep.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request to gateway failed: {e}")
        return None

def worker_task(client_id, parameters_all_round, result_queue):
    """
    子进程执行的任务：运行指定轮数的事务，并将原始结果放入队列。
    """
    client_logs.setup_logging_for_process(script_dir, client_id)
    local_results = []
    for i, params in enumerate(parameters_all_round):
        gateway_response = run_workflow_request(TARGET_WORKFLOW, params)
        if (i + 1) % 10 == 0:
            logging.info(f"Client {client_id}, Round {i+1}")
        if gateway_response and gateway_response.get('status') != 'aborted':
            local_results.append(gateway_response)
    result_queue.put(local_results)

def analyze_and_save_results(all_gateway_results, system_mode):
    """
    对所有成功事务的结果进行分析、计算派生指标并保存。
    """
    if not all_gateway_results:
        logging.error("No successful transactions were collected. Aborting analysis.")
        return

    gateway_df = pd.DataFrame(all_gateway_results)
    successful_txids = gateway_df['transaction_id'].tolist()

    # 1. 从CouchDB批量获取函数级别的延迟数据
    logging.info(f"Fetching function-level latencies for {len(successful_txids)} successful transactions...")
    exec_latencies = repo.get_latencies_for_txs_by_phase(successful_txids, 'exec')
    io_latencies = repo.get_latencies_for_txs_by_phase(successful_txids, 'io')

    # 2. 将函数级延迟整合到DataFrame中
    gateway_df['func_e2e_latency'] = gateway_df['transaction_id'].map(exec_latencies).fillna(0)
    gateway_df['io_latency'] = gateway_df['transaction_id'].map(io_latencies).fillna(0)
    # 3. 计算派生延迟指标
    # scheduling_latency: 工作流总执行时间 - 所有函数执行时间的算术和
    gateway_df['scheduling_latency'] = gateway_df['workflow_exec_latency'] - gateway_df['func_e2e_latency']
    # function_exec_latency: 函数执行时间中的纯计算部分
    gateway_df['function_exec_latency'] = gateway_df['func_e2e_latency'] - gateway_df['io_latency']

    numeric_columns = [
        'e2e_latency', 'workflow_exec_latency', 'rounds',
        'func_e2e_latency', 'io_latency', 'time_commit', 'time_repair',
        'function_exec_latency', 'scheduling_latency', 'time_inside_validator'
    ]
    avg_metrics = gateway_df[numeric_columns].mean()

    #log_message(f"transactio

    # 5. 整理最终的汇总报告
    summary = {
        "e2e_latency": avg_metrics.get("e2e_latency"),
        "scheduling_latency": avg_metrics.get("scheduling_latency"),
        "function_io_latency": avg_metrics.get("io_latency"),
        "function_exec_latency": avg_metrics.get("function_exec_latency"),
        "time_inside_validator": avg_metrics.get("time_inside_validator", 0),
        "time_repair": avg_metrics.get("time_repair", 0),
        "time_commit": avg_metrics.get("time_commit", 0),
        "rounds": avg_metrics.get("rounds"),
    }
    
    summary_df = pd.DataFrame([summary])
    
    # 6. 保存结果到CSV
    mode_name = f"{system_mode}"
    output_dir = script_dir / 'results'
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"{TARGET_WORKFLOW}_{mode_name}_latency_breakdown.csv"
    
    summary_df.to_csv(output_file, index=False)
    logging.info(f"Latency breakdown summary saved to {output_file}")
    print("\n--- Latency Breakdown Summary ---")
    print(summary_df.to_string(index=False))
    print("---------------------------------")


def main(system_mode):
    """主执行函数"""
    logging.info(f"Starting latency breakdown experiment for '{TARGET_WORKFLOW}'")
    logging.info(f"Config: mode={system_mode}, clients={CLIENT_CNT}, rounds={ROUND_PER_CLIENT}")

    # 1. 清理环境
    repo.flush_couchdb_workflow_latency()
    logging.info("Flushed workflow_latency database.")

    # 2. 生成所有客户端和轮次的参数
    parameters_all_clients = generate_param.generate_workflow_inputs_for_clients(
        TARGET_WORKFLOW, CLIENT_CNT, ROUND_PER_CLIENT
    )

    # 3. 并发执行所有事务
    result_queue = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(target=worker_task, args=(i, parameters_all_clients[i], result_queue))
        for i in range(CLIENT_CNT)
    ]
    
    start_time = time.time()
    for p in processes:
        p.start()
    
    all_gateway_results = []
    for _ in range(CLIENT_CNT):
        # get() 会阻塞，直到一个子进程完成任务并将结果放入队列
        all_gateway_results.extend(result_queue.get())
    
    end_time = time.time()
    logging.info(f"All {CLIENT_CNT} clients finished and results collected in {end_time - start_time:.2f} seconds.")

    # 5. 现在所有子进程都已完成其核心任务，可以安全地 join 它们
    for p in processes:
        p.join()

    # 6. 分析并保存结果
    analyze_and_save_results(all_gateway_results, system_mode)


if __name__ == '__main__':
    # --- 可在此处修改运行模式 ---
    # system_mode: "PESSIMISTIC", "OPTIMISTIC"
    main('OCC')