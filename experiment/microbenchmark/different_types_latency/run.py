import threading
import boto3
import sys
import json
import pandas as pd
from numpy import random
import requests
import time
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

def generate_workflow_input(workflow, text_size):
    dataset = dataset_all['small'] if text_size == TEXT_SIZE_SMALL else dataset_all['large']
    all_func = repo.get_all_functions(workflow)
    parameters_input = {'f1': {'payload_size': text_size, 'keys':{func: {} for func in all_func}}}
    for func in all_func:
        zipf_param = 1.1
        dataset_len = len(dataset)
        indices = set()
        while len(indices) < 3:
            idx = random.zipf(zipf_param) - 1
            if 0 <= idx < dataset_len:
                indices.add(idx)
        keys = [dataset[i] for i in indices]
        parameters_input['f1']['keys'][func] = {keys[0]:'R', keys[1]:'R', keys[2]:'W'}
    parameters_input['f1']['keys'] = json.dumps(parameters_input['f1']['keys'])
    return parameters_input

    # print(f"Generated parameters inputs for workflow {workflow}: {parameters_inputs[workflow]}")

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

def analyze_workflow(workflow, text_size):
    parameters_input = generate_workflow_input(workflow, text_size)
    rep = run_workflow(workflow, parameters_input) 
    txid = rep['transaction_id']
    validate_time_inside_validator = rep['validate_time_inside_validator']
    validate_latency = rep['validate_latency'] 
    e2e_latency = rep['e2e_latency']  
    first_run_latency = rep['first_run_latency']
    func_exec_time_test, func_io_time_test = get_function_latency(txid)
    func_io_time = func_io_time_test
    func_exec_time = func_exec_time_test 
    result_dict[txid] = {"first_run_latency":first_run_latency, "validate_time_inside_validator": validate_time_inside_validator, "validate_latency": validate_latency, "e2e_latency": e2e_latency, "func_io_time": func_io_time, "func_exec_time": func_exec_time}

def analyze_all(text_size):
    create_microbenchmark_dataset()
    repo.flush_couchdb_workflow_latency()
    for workflow in all_workflows:
            # 创建线程函数
        def thread_task():
            for _ in range(ROUND):  # 每个线程调用 ROUND 次
                analyze_workflow(workflow, text_size)  # 调用 analyze_workflow

        # 创建4个线程
        threads = []
        for i in range(CLIENT_CNT):
            thread = threading.Thread(target=thread_task)
            threads.append(thread)
            thread.start()
            # 等待所有线程运行结束
        for thread in threads:
            thread.join()
        # 统计 result_dict 中的结果
        validate_time_inside_validator = []
        validate_latency = []
        e2e_latency = []
        first_run_latency = []
        func_io_time = []
        func_exec_time = []

        for result in result_dict.values():
            validate_time_inside_validator.append(result["validate_time_inside_validator"])
            validate_latency.append(result["validate_latency"])
            e2e_latency.append(result["e2e_latency"])
            first_run_latency.append(result["first_run_latency"])
            func_io_time.append(result["func_io_time"])
            func_exec_time.append(result["func_exec_time"])

        # 计算平均值
        mode = "beldi"
        avg_results = {
            'mode': mode,
            "validator overhead": sum(validate_time_inside_validator) / len(validate_time_inside_validator),
            "overall validate latency": sum(validate_latency) / len(validate_latency),
            "e2e latency": sum(e2e_latency) / len(e2e_latency),
            "workflow run latency": sum(first_run_latency) / len(first_run_latency),
            "func io latency": sum(func_io_time) / len(func_io_time),
            "func exec latency": sum(func_exec_time) / len(func_exec_time),
        }

        # 创建 DataFrame
        df = pd.DataFrame([avg_results])
        df.to_csv(f"{script_dir}/{workflow}_{mode}.csv")
   
if __name__ == '__main__':

    TEXT_SIZE = int(sys.argv[1])
    analyze_all(TEXT_SIZE)



