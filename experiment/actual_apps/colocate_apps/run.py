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
# all_workflows = ['social_network']
all_workflows = ['travel_reservation', 'social_network', 'banking_system']
result_dict = {}

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
        
        # 启动所有客户端进程
        for i in range(CLIENT_CNT):
            processes[i].start()
            logging.info(f"Started client process {processes[i].pid} for client {i}")

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
            "workflow_run_latency": latency.get("first_run_latency"),
            'exec_latency': latency.get("exec_latency"),
            'io_latency': latency.get("io_latency")
        }
        logging.info(f"工作流 {workflow} 处理完成, {compute_mode} 结果: {summary}")
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



def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    transaction_id = rep.get('transaction_id', '')
    return transaction_id, {
        "validate_time_inside_validator": rep.get('validate_time_inside_validator', 0),
        "validate_latency": rep.get('validate_latency', 0),
        "e2e_latency": rep.get('e2e_latency', 0),
        "first_run_latency": rep.get('first_run_latency', 0)
    }, rep['status']

def analyze_all_workflows(system_mode, opt, compute_mode):
    """并行分析所有工作流"""
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
    
    # 获取函数延迟数据
    repo = repository.Repository()
    try:
        function_latencies = repo.get_latencies()
        print(f"Function latencies collected: {function_latencies}")
    except Exception as e:
        print(f"Error getting function latencies: {e}")
        function_latencies = {}
    
    # 将函数延迟数据整合到工作流结果中
    for result in workflow_results:
        workflow_name = result['workflow']
        if workflow_name in function_latencies:
            # 计算该工作流的平均执行和IO延迟
            if compute_mode == 'p99':
                if 'exec' in function_latencies[workflow_name]:
                    exec_latencies = function_latencies[workflow_name]['exec']
                    result['function_exec_latency'] = sorted(exec_latencies)[int(len(exec_latencies) * 0.99)] if exec_latencies else 0
                else:
                    result['function_exec_latency'] = 0
                    
                if 'io' in function_latencies[workflow_name]:
                    io_latencies = function_latencies[workflow_name]['io']
                    result['function_io_latency'] = sorted(io_latencies)[int(len(io_latencies) * 0.99)] if io_latencies else 0
                else:
                    result['function_io_latency'] = 0
            else:  # avg
                if 'exec' in function_latencies[workflow_name]:
                    exec_latencies = function_latencies[workflow_name]['exec']
                    result['function_exec_latency'] = sum(exec_latencies) / len(exec_latencies) if exec_latencies else 0
                else:
                    result['function_exec_latency'] = 0
                    
                if 'io' in function_latencies[workflow_name]:
                    io_latencies = function_latencies[workflow_name]['io']
                    result['function_io_latency'] = sum(io_latencies) / len(io_latencies) if io_latencies else 0
                else:
                    result['function_io_latency'] = 0
        else:
            # 如果没有找到对应工作流的函数延迟数据，设置为0
            result['function_exec_latency'] = 0
            result['function_io_latency'] = 0
    
    # 创建汇总的 DataFrame
    if workflow_results:
        summary_df = pd.DataFrame(workflow_results)
        
        # 重新排列列的顺序，包含新的函数延迟列
        columns_order = [
            "workflow", 
            "mode", 
            "validator_overhead", 
            "overall_validate_latency", 
            "e2e_latency", 
            "workflow_run_latency",
            'exec_latency',
            'io_latency',
            'function_exec_latency',  # 新增：从CouchDB获取的函数执行延迟
            'function_io_latency'     # 新增：从CouchDB获取的函数IO延迟
        ]
        summary_df = summary_df[columns_order]
        
        # 保存汇总结果
        output_file = script_dir / 'results' / f'all_workflows_{compute_mode}_summary.csv'
        output_file.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(output_file, index=False)
        
        print(f"\n=== 所有工作流 {compute_mode} 延迟汇总结果 ===")
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



