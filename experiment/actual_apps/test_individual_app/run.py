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
# all_workflows = ['banking_system']
all_workflows = ['social_network']
result_dict = {}


def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程执行的任务。"""
    client_logs.setup_logging_for_process(script_dir, client_id)
    # logging.info(f"Process started for workflow: {workflow}")
    
    local_results = []
    for i in range(ROUND):
        # 注意：analyze_workflow 需要能被子进程调用，并且其内部逻辑是进程安全的
        # 这里假设 analyze_workflow 返回一个包含结果的字典
        txid, result, tx_res = analyze_workflow(workflow, parameters_all_round[i])
        local_results.append(result)
        if i % 10 == 0:
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} completed for workflow {workflow}, txid: {txid}, result: {result}")
        # logging.info(f"[{txid}] Finished, tx_res: {tx_res}")

    result_queue.put(local_results)
    logging.info("Process finished.")

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
    # func_exec_time_test, func_io_time_test = get_function_latency(rep['transaction_id'])
    # func_io_time = func_io_time_test
    # func_exec_time = func_exec_time_test 
    # rep['func_io_time'] = func_io_time
    # rep['func_exec_time'] = func_exec_time
    return rep['transaction_id'], {
        "validate_time_inside_validator": rep['validate_time_inside_validator'],
        "validate_latency": rep['validate_latency'],
        "e2e_latency": rep['e2e_latency'],
        "first_run_latency": rep['first_run_latency'],
    }, rep['res']

def analyze_all(system_mode,opt, compute_mode='avg'):
    repo.flush_couchdb_workflow_latency()
    for workflow in all_workflows:
        parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, CLIENT_CNT, ROUND)
        result_queue = multiprocessing.Queue()
        # 创建4个线程
        processes = []
        for i in range(CLIENT_CNT):
            process = multiprocessing.Process(
                target=worker_task, 
                args=(i, workflow, parameters_all[i], result_queue)
            )
            processes.append(process)
        
        for i in range(CLIENT_CNT):
            processes[i].start()
            print(f"Started process {processes[i].pid} for client {i}")

        # 等待所有子进程运行结束
        for process in processes:
            process.join()

        # 从队列中收集所有结果
        all_results = []
        while not result_queue.empty():
            all_results.extend(result_queue.get())

        # 统计 result_dict 中的结果
        if not all_results:
            print(f"No results collected for workflow {workflow}. Skipping.")
            continue

        df = pd.DataFrame(all_results)
        
        # 计算99%-ile延迟
        mode = f"[PRE_CREATE]_{system_mode}_{opt}_{compute_mode}"
        if compute_mode == 'avg':
            avg_latency = df.mean()

            summary = {
                "mode": mode,
                "validator overhead": avg_latency.get("validate_time_inside_validator"),
                "overall validate latency": avg_latency.get("validate_latency"),
                "e2e latency": avg_latency.get("e2e_latency"),
                "workflow run latency": avg_latency.get("first_run_latency")
            }

            summary_df = pd.DataFrame([summary])
            output_file = script_dir / 'results' /f"{workflow}_{mode}.csv"
            summary_df.to_csv(output_file, index=False)
        elif compute_mode == '99p':
            p99_latency = df.quantile(0.99)

            summary = {
                "mode": mode,
                "validator overhead": p99_latency.get("validate_time_inside_validator"),
                "overall validate latency": p99_latency.get("validate_latency"),
                "e2e latency": p99_latency.get("e2e_latency"),
                "workflow run latency": p99_latency.get("first_run_latency")
            }

            summary_df = pd.DataFrame([summary])
            output_file = script_dir / 'results' /f"{workflow}_{mode}.csv"
            summary_df.to_csv(output_file, index=False)
        print(f"[{workflow}] Results summary saved to {output_file}")

system_mode = ["PESSIMISTIC", "OPTIMISTIC"]
opt = ['basic', 'fast-path']

if __name__ == '__main__':
    _system_mode= system_mode[0]
    _opt = opt[1] 
    compute_mode = sys.argv[1] if len(sys.argv) > 1 else 'avg'
    analyze_all(_system_mode, _opt, compute_mode=compute_mode)



