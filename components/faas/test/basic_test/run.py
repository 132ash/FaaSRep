from gevent import monkey
monkey.patch_all()

import sys
import pandas as pd
import requests

sys.path.append('..')
sys.path.append('../../config')
from repository import Repository
import time


import config
repo = Repository()

workflow_pool = ["simpleseq"]

TEST_TIME = 1


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


def analyze_workflow(workflow_name):
    print(f'----analyzing {workflow_name}----')
    repo.flush_couchdb_workflow_latency()

    tested = 0
    validate_latency = 0
    e2e_latency = 0
    func_io_time = 0
    func_exec_time = 0
    while tested < TEST_TIME:
        print(f"testing {workflow_name} {tested + 1} time")
        rep = run_workflow(workflow_name)
        txid = rep['transaction_id']
        validate_latency += rep['validate_latency']
        e2e_latency += rep['e2e_latency']
        func_exec_time_test, func_io_time_test = get_function_latency(txid)
        func_io_time += func_io_time_test
        func_exec_time += func_exec_time_test
        tested += 1
    
    return validate_latency / TEST_TIME, e2e_latency / TEST_TIME, func_io_time / TEST_TIME, func_exec_time / TEST_TIME
    

def analyze(mode):
    validate_overall = []
    e2e_overall = []
    func_io_overall = []
    func_exec_overall = []
    for workflow in workflow_pool:
        validate_latency, e2e_latency, func_io_time, func_exec_time = analyze_workflow(workflow)
        validate_overall.append(validate_latency)
        e2e_overall.append(e2e_latency)
        func_io_overall.append(func_io_time)
        func_exec_overall.append(func_exec_time)
        print(f"workflow {workflow} finished with validate_latency {validate_latency}, e2e_latency {e2e_latency}, func_io_time {func_io_time}, func_exec_time {func_exec_time}")
    df = pd.DataFrame({'workflow': workflow_pool, 'validate_latency': validate_overall, 'e2e_latency': e2e_overall, 'func_io_time': func_io_overall, 'func_exec_time': func_exec_overall})
    df.to_csv(mode + '.csv')
   


if __name__ == '__main__':
    mode = sys.argv[1]
    analyze(mode)



