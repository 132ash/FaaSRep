import sys
import pandas as pd
import requests
import time


sys.path.append('../config')
sys.path.append('..')
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
    func_exec_latency, func_io_latency = repo.get_latencies(txid, 'exec'), repo.get_function_latency(txid, 'io')
    exec_time = sum([i['time'] for i in func_exec_latency]) 
    io_time = sum([i['time'] for i in func_io_latency]) 
    return exec_time, io_time


def analyze_workflow(workflow_name):
    print(f'----analyzing {workflow_name}----')
    repo.clear_couchdb_results()
    repo.clear_couchdb_workflow_latency()

    tested = 0
    validate_latency = 0
    exec_latency = 0
    func_io_time = 0
    func_exec_time = 0
    while tested < TEST_TIME:
        print(f"testing {workflow_name} {tested + 1} time")
        rep = run_workflow(workflow_name)
        txid = rep['txid']
        validate_latency += rep['validate_latency']
        exec_latency += rep['exec_latency']
        func_io_time_test, func_exec_time_test = get_function_latency(txid)
        func_io_time += func_io_time_test
        func_exec_time += func_exec_time_test
        tested += 1
    
    return validate_latency / TEST_TIME, exec_latency / TEST_TIME, func_io_time / TEST_TIME, func_exec_time / TEST_TIME
    

def analyze(mode):
    validate_overall = []
    exec_overall = []
    func_io_overall = []
    func_exec_overall = []
    for workflow in workflow_pool:
        validate_latency, exec_latency, func_io_time, func_exec_time = analyze_workflow(workflow)
        validate_overall.append(validate_latency)
        exec_overall.append(exec_latency)
        func_io_overall.append(func_io_time)
        func_exec_overall.append(func_exec_time)
        print(f"workflow {workflow} finished with validate_latency {validate_latency}, exec_latency {exec_latency}, func_io_time {func_io_time}, func_exec_time {func_exec_time}")
    df = pd.DataFrame({'workflow': workflow_pool, 'validate_latency': validate_overall, 'exec_latency': exec_overall, 'func_io_time': func_io_overall, 'func_exec_time': func_exec_overall})
    df.to_csv(mode + '.csv')
   


if __name__ == '__main__':
    mode = sys.argv[1]
    analyze(mode)



