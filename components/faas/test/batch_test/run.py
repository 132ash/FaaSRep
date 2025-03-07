import gevent
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

batch = ["sectestflow", "testflow"]

TEST_TIME = 1

parameters_input = {
    "testflow": {
        "func1": {"chained_num_0": 1}
    }, 
    "sectestflow": {
        "f1": {"chained_num_0": 1}
    }
}



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
            rep = result[workflow]
            txid = rep['transaction_id']
            validate_latency[workflow] += (rep['validate_latency'] / TEST_TIME)
            e2e_latency[workflow] += (rep['e2e_latency']  / TEST_TIME)
            func_exec_time_test, func_io_time_test = get_function_latency(txid)
            func_io_time[workflow] += (func_io_time_test / TEST_TIME)
            func_exec_time[workflow] += (func_exec_time_test / TEST_TIME)
        tested += 1
    
    return validate_latency, e2e_latency, func_io_time, func_exec_time
    

def analyze(mode='batch'):
    validate_overall = []
    e2e_overall = []
    func_io_overall = []
    func_exec_overall = []
    validate_latency, e2e_latency, func_io_time, func_exec_time = analyze_batch(batch)
    for workflow in batch:
        validate_overall.append(validate_latency[workflow])
        e2e_overall.append(e2e_latency[workflow])
        func_io_overall.append(func_io_time[workflow])
        func_exec_overall.append(func_exec_time[workflow])
        print(f"workflow {workflow} finished with validate_latency {validate_latency}, e2e_latency {e2e_latency}, func_io_time {func_io_time}, func_exec_time {func_exec_time}")
    df = pd.DataFrame({'workflow': batch, 'validate_latency': validate_overall, 'e2e_latency': e2e_overall, 'func_io_time': func_io_overall, 'func_exec_time': func_exec_overall})
    df.to_csv(mode + '.csv')
   


if __name__ == '__main__':
    mode = sys.argv[1]
    analyze(mode)



