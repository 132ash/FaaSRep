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
#all_workflows = ['social_network', 'banking_system', 'travel_reservation']
all_workflows = ['travel_reservation']

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
    local_results = []
    for i in range(ROUND):
        transaction_id = parameters_all_round[i]['transaction_id']
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if tx_status == 'aborted':
            continue
        else:
            local_results.append(result)
        if i % (ROUND // 10) == 0:
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} for workflow {workflow}, txid:{transaction_id}")
    result_queue.put(local_results)

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

        # 从队列中收集所有结果
        all_results = []
        while not result_queue.empty():
            all_results.extend(result_queue.get())

        logging.info(f"工作流 {workflow} 处理完成, 收集到 {len(all_results)} 条有效结果，耗时 {total_time:.2f} 秒。")
        # 将原始结果和总时间放入主队列
        workflow_result_queue.put((workflow, all_results, total_time))
        
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
        "e2e_latency": rep.get('e2e_latency', 0),
        'rounds': rep.get('rounds', 0)
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
        
    # 启动所有工作流进程
    for i, process in enumerate(workflow_processes):
        process.start()
        logging.info(f"Started workflow process {process.pid} for {all_workflows[i]}")
    
    # --- 关键修复：先从队列获取结果，再 join 进程 ---
    # 收集所有工作流的原始结果。这将阻塞直到所有工作流进程都已 put 了它们的结果。
    raw_workflow_results = []
    for _ in all_workflows:
        raw_workflow_results.append(workflow_result_queue.get())

    # 等待所有工作流进程完成它们的清理工作并终止
    for i, process in enumerate(workflow_processes):
        process.join()
        logging.info(f"Workflow process for {all_workflows[i]} completed")
    
    # --- 结果处理和汇总 ---
    final_summary_list = []
    
    # 创建结果目录
    mode_results_dir = script_dir / 'results' / system_mode
    raw_details_dir = mode_results_dir / "raw_details"
    raw_details_dir.mkdir(parents=True, exist_ok=True)

    for workflow, results_list, total_time in raw_workflow_results:
        if not results_list:
            logging.warning(f"No valid results collected for workflow {workflow} in mode {system_mode}")
            continue
            
        df = pd.DataFrame(results_list)
        
        # 1. 存储每次运行的详细结果到单独文件
        raw_output_file = raw_details_dir / f"{workflow}_raw_details.csv"
        df.to_csv(raw_output_file, index=False)
        logging.info(f"Saved raw results for {workflow} to {raw_output_file}")
        
        # 2. 计算 P50, P99 延迟和平均吞吐量
        p50_latency = df['e2e_latency'].quantile(0.50)
        p99_latency = df['e2e_latency'].quantile(0.99)
        # 吞吐量 = 成功事务数 / 总时间
        avg_throughput = len(df) / total_time if total_time > 0 else 0

        avg_rounds = df['rounds'].mean()
        # 关键修改：计算 p99 rounds
        p99_rounds = df['rounds'].quantile(0.99)
        
        summary_dict = {
            'application': workflow,
            'p50_e2e_latency': p50_latency,
            'p99_e2e_latency': p99_latency,
            'avg_throughput': avg_throughput,
            'avg_rounds': avg_rounds,
            # 关键修改：添加 p99_rounds 到汇总字典
            'p99_rounds': p99_rounds
        }
        final_summary_list.append(summary_dict)
        
    # 3. 创建并保存当前模式的汇总文件
    if not final_summary_list:
        logging.error(f"Failed to collect any results for mode {system_mode}")
        return None
        
    summary_df = pd.DataFrame(final_summary_list).sort_values('application')
    
    summary_output_file = script_dir / 'results' / f"{system_mode}_summary.csv"
    summary_df.to_csv(summary_output_file, index=False)
    
    print(f"\n--- {system_mode} Mode Summary ---")
    print(summary_df.to_string(index=False))
    print(f"Summary for {system_mode} mode saved to: {summary_output_file}")
    
    return summary_df

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
