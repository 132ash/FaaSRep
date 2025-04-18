import threading

import boto3
import random
import string
import sys
import pandas as pd
import requests

sys.path.append('..')
sys.path.append('../../config')
from repository import Repository
import time
import config

repo = Repository()
TEXT_SIZE = 4 * 1024 
dynamodb  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"
table_name = "data"
# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table(table_name)

def generate_random_text(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

parameters_input = {
    "textseq": {
        "f1": {"t0": generate_random_text(TEXT_SIZE)}
    },
}

baseline = ["repair", "repair+batch",  "repair+batch+fastpath", "remote lock"]
mode = ["NOCACHE + SMALL", "CACHE + LARGE", "CACHE + SMALL","NOCACHE + LARGE"]

result_dict = {}

def release_lock():
    table = dynamodb.Table("data")
    # 使用 scan 获取所有项并更新 lock 属性为 none
    response = table.scan()
    for item in response.get('Items', []):
        try:
            table.update_item(
                Key={'key': item['key']},
                UpdateExpression="SET #l = :none",
                ExpressionAttributeNames={
                    '#l': 'lock'
                },
                ExpressionAttributeValues={
                    ':none': None  # 使用 None 而不是字符串 'None'
                },
                ReturnValues="UPDATED_NEW"
            )
        except Exception as e:
            print(f"Failed to release lock for key {item['key']}: {e}")
            

def run_workflow(workflow_name, parameters = {}):
    url = 'http://' + config.GATEWAY_ADDR + '/run'
    data = {'workflow':workflow_name, "parameters":parameters}
    rep = requests.post(url, json=data)
    return rep.json()

def get_function_latency(txid):
    func_exec_latency, func_io_latency = repo.get_latencies(txid, 'exec'), repo.get_latencies(txid, 'io')
    exec_time = sum(func_exec_latency) 
    io_time = sum(func_io_latency) 
    print(f"func_exec_latency: {func_exec_latency}")
    print(f"func_io_latency: {func_io_latency}")
    return exec_time, io_time


def analyze_workflow(workflow):
    rep = run_workflow(workflow, parameters_input[workflow]) 
    txid = rep['transaction_id']
    validate_time_inside_validator = rep['validate_time_inside_validator']
    validate_latency = rep['validate_latency'] 
    e2e_latency = rep['e2e_latency']  
    first_run_latency = rep['first_run_latency']
    func_exec_time_test, func_io_time_test = get_function_latency(txid)
    func_io_time = func_io_time_test
    func_exec_time = func_exec_time_test 
    result_dict[txid] = {"first_run_latency":first_run_latency, "validate_time_inside_validator": validate_time_inside_validator, "validate_latency": validate_latency, "e2e_latency": e2e_latency, "func_io_time": func_io_time, "func_exec_time": func_exec_time}

    

def analyze_all(_baseline, _mode):
    repo.flush_couchdb_workflow_latency()

        # 创建线程函数
    def thread_task():
        for _ in range(10):  # 每个线程调用 10 次
            analyze_workflow("textseq")  # 调用 analyze_workflow
            time.sleep(0.05)  # 每隔 50ms 调用一次

        # 创建三个线程
    threads = []
    for _ in range(3):
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
    avg_results = {
        "validator overhead": sum(validate_time_inside_validator) / len(validate_time_inside_validator),
        "overall validate latency": sum(validate_latency) / len(validate_latency),
        "e2e latency": sum(e2e_latency) / len(e2e_latency),
        "workflow run latency": sum(first_run_latency) / len(first_run_latency),
        "func io latency": sum(func_io_time) / len(func_io_time),
        "func exec latency": sum(func_exec_time) / len(func_exec_time),
    }

    # 创建 DataFrame
    df = pd.DataFrame([avg_results])
    df.to_csv(f"{_baseline}_{_mode}" + '.csv')
   


if __name__ == '__main__':
    _baseline = baseline[0]
    _mode = mode[0]
    if mode == "remote lock":
        release_lock()
    analyze_all(_baseline, _mode)



