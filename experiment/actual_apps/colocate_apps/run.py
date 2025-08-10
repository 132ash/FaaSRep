import boto3
import logging
import json
import sys

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

import pandas as pd
import multiprocessing
import requests
import config.config as config
from experiment.common import repository, client_logs
from experiment.common import generate_param
repo = repository.Repository()


DB_NODE_IP = config.STOREGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"

# travel_reservation_config
FLIGHT_IDS = config.FLIGHT_IDS
FLIGHT_CAPACITY = config.FLIGHT_CAPACITY
RENTAL_START = config.RENTAL_START
RENTAL_END = config.RENTAL_END
DATE_FORMAT = config.DATE_FORMAT

CLIENT_CNT = 16
ROUND = 50
parameters_inputs = {}
all_workflows = ['social_network']
#all_workflows = ['travel_reservation', 'social_network', 'banking_system']
result_dict = {}


def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程中的客户端任务。"""
    client_logs.setup_logging_for_process(script_dir, client_id)
    
    local_results = []
    for i in range(ROUND):
        transaction_id = parameters_all_round[i]['transaction_id']
        #logging.info(f"[{client_id}] Round {i+1}/{ROUND} for workflow {workflow}, txid:{transaction_id}")
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if tx_status == 'aborted':
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} aborted for workflow {workflow}, txid: {txid}")
        else:
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} completed for workflow {workflow}, txid: {txid}, result: {result}")
            local_results.append(result)
    result_queue.put(local_results)

def workflow_process_task(workflow, workflow_result_queue, sys_mode, compute_mode):
    """每个工作流的主进程任务。"""
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
        
        #logging.info(f"开始处理工作流: {workflow}")
        
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
        
        # 启动所有客户端进程
        for i in range(CLIENT_CNT):
            processes[i].start()
            #logging.info(f"Started client process {processes[i].pid} for client {i}")

        # 等待所有客户端进程结束
        for process in processes:
            process.join()

        # 从队列中收集所有结果
        all_results = []
        while not result_queue.empty():
            all_results.extend(result_queue.get())

        # 统计结果
        if not all_results:
            logging.warning(f"No results collected for workflow {workflow}")
            workflow_result_queue.put((workflow, None))
            return

        df = pd.DataFrame(all_results)
        
        # 计算99%-ile延迟
        if compute_mode == 'p99':
            latency = df.quantile(0.99)
        else:
            latency = df.mean()
        summary = {
            "workflow": workflow,
            "mode": sys_mode,
            "validator_overhead": latency.get("validate_time_inside_validator"),
            "overall_validate_latency": latency.get("validate_latency"),
            "e2e_latency": latency.get("e2e_latency"),
            "workflow_run_latency": latency.get("first_run_latency")
        }
        #logging.info(f"工作流 {workflow} 处理完成, {compute_mode} 结果: {summary}")
        workflow_result_queue.put((workflow, summary))
        
    except Exception as e:
        logging.error(f"工作流 {workflow} 处理出错: {e}")
        workflow_result_queue.put((workflow, None))

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    transaction_id = parameters.pop('transaction_id', None)
    if transaction_id:
        inputs['transaction_id'] = transaction_id
    rep = requests.post(url, json = inputs)
    return rep.json()

def get_function_latency(txid):
    func_exec_latency, func_io_latency = repo.get_latencies(txid, 'exec'), repo.get_latencies(txid, 'io')
    exec_time = sum(func_exec_latency) 
    io_time = sum(func_io_latency) 
    return exec_time, io_time

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    return rep.get('transaction_id', ''), {
        "validate_time_inside_validator": rep.get('validate_time_inside_validator', 0),
        "validate_latency": rep.get('validate_latency', 0),
        "e2e_latency": rep.get('e2e_latency', 0),
        "first_run_latency": rep.get('first_run_latency', 0),
    }, rep['status']

def analyze_all_workflows(system_mode, opt, compute_mode):
    """并行分析所有工作流"""
    # 清理之前的延迟数据
    repo.flush_couchdb_workflow_latency()
    
    # 创建工作流结果队列
    workflow_result_queue = multiprocessing.Queue()
    
    # 为每个工作流创建一个独立的进程
    workflow_processes = []
    for workflow in all_workflows:
        process = multiprocessing.Process(
            target=workflow_process_task,
            args=(workflow, workflow_result_queue,f"{system_mode}_{opt}", compute_mode)
        )
        workflow_processes.append(process)
        
    # 启动所有工作流进程
    for i, process in enumerate(workflow_processes):
        process.start()
        print(f"Started workflow process {process.pid} for {all_workflows[i]}")
    
    # 等待所有工作流进程完成
    for i, process in enumerate(workflow_processes):
        process.join()
        print(f"Workflow process for {all_workflows[i]} completed")
    
    # 收集所有工作流的结果
    workflow_results = []
    while not workflow_result_queue.empty():
        workflow, result = workflow_result_queue.get()
        if result is not None:
            workflow_results.append(result)
        else:
            print(f"Warning: No valid results for workflow {workflow}")
    
    # 创建汇总的 DataFrame
    if workflow_results:
        summary_df = pd.DataFrame(workflow_results)
        
        # 重新排列列的顺序
        columns_order = [
            "workflow", 
            "mode", 
            "validator_overhead", 
            "overall_validate_latency", 
            "e2e_latency", 
            "workflow_run_latency"
        ]
        summary_df = summary_df[columns_order]
        
        # 保存汇总结果
        output_file = script_dir / 'results' / 'all_workflows_99p_summary.csv'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(output_file, index=False)
        
        print(f"\n=== 所有工作流 99%-ile 延迟汇总结果 ===")
        print(summary_df.to_string(index=False))
        print(f"\n结果已保存到: {output_file}")
        
        # 同时保存每个工作流的单独结果
        for result in workflow_results:
            workflow_name = result['workflow']
            single_workflow_df = pd.DataFrame([result])
            single_output_file = script_dir / 'results' / f"{workflow_name}_{system_mode}_{opt}_{compute_mode}.csv"
            single_workflow_df.to_csv(single_output_file, index=False)
            print(f"单独结果已保存到: {single_output_file}")
        
        return summary_df
    else:
        print("Error: No valid results collected from any workflow")
        return None

system_mode = ["PESSIMISTIC", "OPTIMISTIC"]
opt = ['basic', 'fast-path']

if __name__ == '__main__':
    _system_mode= system_mode[1]
    _opt = opt[1] 
    compute_mode = sys.argv[1] if len(sys.argv) > 1 else 'avg'
    results_dir = script_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # 分析所有工作流
    summary_df = analyze_all_workflows(_system_mode, _opt, compute_mode=compute_mode)    
    if summary_df is not None:
        print("\n=== 执行完成 ===")
        print("所有工作流已并行处理完成，结果已保存")
    else:
        print("\n=== 执行失败 ===")
        print("未能收集到有效结果")



