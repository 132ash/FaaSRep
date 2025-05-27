import gevent
from gevent import monkey
monkey.patch_all()

import boto3

import sys
import pandas as pd
import requests

sys.path.append('..')
sys.path.append('../../config')
from repository import Repository
import time


import config
repo = Repository()

batches = [["testflow", "sectestflow"]]


TEST_TIME = 1

parameters_input = {
    "testflow": {
        "func1": {"chained_num_0": 1}
    }, 
    "sectestflow": {
        "f1": {"chained_num_0": 1}
    }
}

dynamodb  = boto3.resource('dynamodb', endpoint_url='http://192.168.162.132:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"
table_name = "data"
# 创建名为data的表，以字符串key作为键，每个键对应version和value两个字段，都是字符串
table = dynamodb.Table(table_name)


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
            

def run_workflow(workflow_name, parameters = {}, result={}):
    url = 'http://' + config.GATEWAY_ADDR + '/run'
    data = {'workflow':workflow_name, "parameters":parameters}
    rep = requests.post(url, json=data)
    result[workflow_name] = rep.json()

def get_function_latency(txid):
    func_exec_latency, func_io_latency = repo.get_latencies(txid, 'exec'), repo.get_latencies(txid, 'io')
    exec_time = sum(func_exec_latency) 
    io_time = sum(func_io_latency) 
    print(f"func_exec_latency: {func_exec_latency}")
    print(f"func_io_latency: {func_io_latency}")
    return exec_time, io_time


def analyze_batch(batch):
    print(f'----analyzing {batch}----')
    repo.flush_couchdb_workflow_latency()

    tested = 0
    validate_latency = {workflow:0 for workflow in batch}
    validate_time_inside_validator = {workflow:0 for workflow in batch}
    e2e_latency = {workflow:0 for workflow in batch}
    func_io_time = {workflow:0 for workflow in batch}
    func_exec_time = {workflow:0 for workflow in batch}
    while tested < TEST_TIME:
        result = {}
        jobs = []
        print(f"testing {batch} {tested + 1} time")
        for workflow in batch:
            jobs.append(gevent.spawn(run_workflow, workflow, parameters_input[workflow], result))
        gevent.joinall(jobs) 
        for workflow in batch:
            while not result.get(workflow, False):
                time.sleep(0.1)
            rep = result[workflow]
            txid = rep['transaction_id']
            validate_time_inside_validator[workflow] += (rep['validate_time_inside_validator'] / TEST_TIME)
            validate_latency[workflow] += (rep['validate_latency'] / TEST_TIME)
            e2e_latency[workflow] += (rep['e2e_latency']  / TEST_TIME)
            func_exec_time_test, func_io_time_test = get_function_latency(txid)
            func_io_time[workflow] += (func_io_time_test / TEST_TIME)
            func_exec_time[workflow] += (func_exec_time_test / TEST_TIME)
        tested += 1
    
    return validate_time_inside_validator, validate_latency, e2e_latency, func_io_time, func_exec_time
    

def analyze(mode='batch'):
    for batch in batches:
        validate_time_inside_validator_overall = []
        validate_overall = []
        e2e_overall = []
        func_io_overall = []
        func_exec_overall = []
        validate_time_inside_validator, validate_latency, e2e_latency, func_io_time, func_exec_time = analyze_batch(batch)
        for workflow in batch:
            validate_time_inside_validator_overall.append(validate_time_inside_validator[workflow])
            validate_overall.append(validate_latency[workflow])
            e2e_overall.append(e2e_latency[workflow])
            func_io_overall.append(func_io_time[workflow])
            func_exec_overall.append(func_exec_time[workflow])
            print(f"workflow {workflow} finished with  validate_time_inside_validator{ validate_time_inside_validator}, validate_latency {validate_latency}, e2e_latency {e2e_latency}, func_io_time {func_io_time}, func_exec_time {func_exec_time}")
        df = pd.DataFrame({'workflow': batch,'validate_time_inside_validator':validate_time_inside_validator_overall, 'validate_latency': validate_overall, 'e2e_latency': e2e_overall, 'func_io_time': func_io_overall, 'func_exec_time': func_exec_overall})
        df.to_csv(str(batch)+mode + '.csv')
   


if __name__ == '__main__':
    # mode = sys.argv[1]
    release_lock()
    analyze()



