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
from repository import Repository
repo = Repository()



DB_NODE_IP = config.STOREGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"
table_name = "data"
# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table(table_name)

ROUND = 1000
TEXT_SIZE = 4 * 1024
parameters_inputs = {}
all_workflows = ['c4']
result_dict = {}

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
    local_results = []
    for i in range(ROUND):
        start_time = pd.Timestamp.now()
        txid, result = analyze_workflow(workflow, parameters_all_round[i])
        end_time = pd.Timestamp.now()
        
        # 计算实际测试时间
        test_duration = (end_time - start_time).total_seconds()
        result['test_duration'] = test_duration
        result['client_id'] = client_id
        result['round'] = i + 1
        
        local_results.append(result)
        if i % (ROUND // 10) == 0:
            print(f"Client {client_id}: Round {i+1} completed")
    result_queue.put(local_results)

def generate_workflow_inputs_for_clients(workflow, dataset_all, client_cnt):
    all_func = repo.get_all_functions(workflow)
    client_round_inputs = []
    for client_id in range(client_cnt):
        round_inputs = []
        for round_id in range(ROUND):
            parameters_input = {'f1': {'payload_size': TEXT_SIZE, 'keys': {func: {} for func in all_func}}}
            for func in all_func:
                zipf_param = 1.1
                dataset_len = len(dataset_all)
                indices = set()
                while len(indices) < 3:
                    idx = random.zipf(zipf_param) - 1
                    if 0 <= idx < dataset_len:
                        indices.add(idx)
                keys = [dataset_all[i] for i in indices]
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

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    return rep['transaction_id'], {
        "validate_time_inside_validator": rep['validate_time_inside_validator'],
        "validate_latency": rep['validate_latency'],
        "e2e_latency": rep['e2e_latency'],
        "first_run_latency": rep['first_run_latency'],
    }

def analyze_all(workflow_name, system_mode, client_cnt):
    print(f"开始测试 - 工作流: {workflow_name}, 模式: {system_mode}, 客户端: {client_cnt}")
    repo.flush_couchdb_workflow_latency()
    DS_JSON_PATH  = ROOT_DIR / "experiment/microbenchmark/db_keys.json"
    dataset_all = json.load(open(DS_JSON_PATH, 'r', encoding='utf-8'))
    parameters_all = generate_workflow_inputs_for_clients(workflow_name, dataset_all, client_cnt)
    result_queue = multiprocessing.Queue()
    # 创建client_cnt个线程
    processes = []
    for i in range(client_cnt):
        process = multiprocessing.Process(
            target=worker_task, 
            args=(i, workflow_name, parameters_all[i], result_queue)
        )
        processes.append(process)
    
    for i in range(client_cnt):
        processes[i].start()
        print(f"Started process {processes[i].pid} for client {i}")

    # 等待所有子进程运行结束
    for process in processes:
        process.join()

    # 从队列中收集所有结果
    all_results = []
    while not result_queue.empty():
        client_results = result_queue.get()
        all_results.extend(client_results)

    # 统计 result_dict 中的结果
    if not all_results:
        raise Exception(f"No results collected for workflow {workflow_name}.")
        
    df = pd.DataFrame(all_results)
    
    median_e2e_latency = df['e2e_latency'].quantile(0.50)
    
    # 计算平均吞吐量: client_count / 平均延迟
    avg_e2e_latency = df['e2e_latency'].mean()
    avg_throughput = (client_cnt * 1000) / avg_e2e_latency  # 转换为 RPS (延迟单位是ms)
    
    print(f"")
    print(f"📊 {workflow_name} 测试结果:")
    print(f"   总请求数: {len(all_results)}")
    print(f"   中位数 E2E 延迟: {median_e2e_latency:.2f} ms")
    print(f"   平均 E2E 延迟: {avg_e2e_latency:.2f} ms")
    print(f"   平均吞吐量: {avg_throughput:.2f} RPS")

    print(f"RESULT:{workflow_name},{client_cnt},{median_e2e_latency:.2f},{avg_throughput:.2f}")
    
    return median_e2e_latency, avg_throughput

if __name__ == '__main__':
    workflow_name = sys.argv[1]
    system_mode = sys.argv[2]
    client_cnt = int(sys.argv[3])
    analyze_all(workflow_name, system_mode, client_cnt)



