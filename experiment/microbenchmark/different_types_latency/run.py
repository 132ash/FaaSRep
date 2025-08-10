import threading
import boto3
import sys
import json
import logging
import pandas as pd
import multiprocessing
from numpy import random
import requests
import numpy as np
from pathlib import Path

def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

script_dir = Path(__file__).parent
ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR / 'config'))
sys.path.append(str(ROOT_DIR / 'experiment'))
sys.path.append(str(ROOT_DIR / 'experiment' / 'microbenchmark'))
import config
from DB_setup import create_microbenchmark_dataset
from repository import Repository
repo = Repository()



DB_NODE_IP = config.STOREGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"
table_name = "data"
# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table(table_name)

TEXT_SIZE_SMALL = 8
TEXT_SIZE_LARGE = 8 * 1024  # 8B / 8KB
CLIENT_CNT = 9
ROUND = 10
parameters_inputs = {}
all_workflows = ['c4']
result_dict = {}

DS_JSON_PATH  = ROOT_DIR / "experiment/microbenchmark/db_keys.json"
dataset_all = json.load(open(DS_JSON_PATH, 'r', encoding='utf-8'))

def setup_logging_for_process(client_id):
    """为每个子进程配置独立的日志文件。"""
    log_dir = script_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"client_{client_id}.log"
    
    # 移除旧的 handlers，为子进程设置新的
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [Client-{client_id}] [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler(sys.stdout) # 也可以同时输出到控制台
        ]
    )

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程执行的任务。"""
    setup_logging_for_process(client_id)
    # #logging.info(f"Process started for workflow: {workflow}")
    
    local_results = []
    for i in range(ROUND):
        #logging.info(f"Starting round {i+1}/{ROUND}")
        # 注意：analyze_workflow 需要能被子进程调用，并且其内部逻辑是进程安全的
        # 这里假设 analyze_workflow 返回一个包含结果的字典
        txid, result = analyze_workflow(workflow, parameters_all_round[i])
        local_results.append(result)
        #logging.info(f"Finished round {i+1}/{ROUND} with txid: {txid}")

    result_queue.put(local_results)
    #logging.info("Process finished.")

def generate_workflow_inputs_for_clients(workflow, text_size):
    dataset = dataset_all['small'] if text_size == TEXT_SIZE_SMALL else dataset_all['large']
    all_func = repo.get_all_functions(workflow)
    client_round_inputs = []
    for client_id in range(CLIENT_CNT):
        round_inputs = []
        for round_id in range(ROUND):
            parameters_input = {'f1': {'payload_size': text_size, 'keys': {func: {} for func in all_func}}}
            for func in all_func:
                zipf_param = 1.1
                dataset_len = len(dataset)
                indices = set()
                while len(indices) < 3:
                    idx = random.zipf(zipf_param) - 1
                    if 0 <= idx < dataset_len:
                        indices.add(idx)
                keys = [dataset[i] for i in indices]
                parameters_input['f1']['keys'][func] = {keys[0]: 'R', keys[1]: 'R', keys[2]: 'W'}
            parameters_input['f1']['keys'] = json.dumps(parameters_input['f1']['keys'])
            round_inputs.append(parameters_input)
        client_round_inputs.append(round_inputs)
    return client_round_inputs

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
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
    }

def analyze_all(_system_mode, _opt, text_size):
    create_microbenchmark_dataset()
    repo.flush_couchdb_workflow_latency()
    for workflow in all_workflows:
        parameters_all = generate_workflow_inputs_for_clients(workflow, text_size)
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
        mode = f"{_system_mode}_{_opt}"
        avg_latency = df.mean()

        summary = {
            "mode": mode,
            "validator overhead": avg_latency.get("validate_time_inside_validator"),
            "overall validate latency": avg_latency.get("validate_latency"),
            "e2e latency": avg_latency.get("e2e_latency"),
            "workflow run latency": avg_latency.get("first_run_latency")
        }
        
        summary_df = pd.DataFrame([summary])
        output_file = script_dir / f"{workflow}_{mode}.csv"
        summary_df.to_csv(output_file, index=False)
        
        print(f"Results summary saved to {output_file}")
   
system_mode = ["PESSIMISTIC", "OPTIMISTIC"]
opt = ['basic', 'fast-path']

if __name__ == '__main__':
    _system_mode= system_mode[1]
    _opt = opt[0] 
    TEXT_SIZE = int(sys.argv[1])
    analyze_all(_system_mode, _opt, TEXT_SIZE)



