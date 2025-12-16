from gevent import monkey
monkey.patch_all()
import gevent
import boto3
import datetime
import json
import sys
import time
import os
import pandas as pd
from tqdm import tqdm
import requests

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

import config.config as config
from experiment.common import generate_param
from gevent.pool import Pool

try:
    with open(script_dir / 'trace_tidy.json', 'r') as f:
        raw_trace = json.load(f)
except Exception as e:
    print(f"Error loading trace file: {e}")
    sys.exit(1)

DB_NODE_IP = config.STORAGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# --- 全局测试参数 ---
workflow = 'travel_reservation'
mode='PESSIMISTIC'
ids = {}
latencies = []
firing_timestamps = []

def post_request(workflow, request_id, parameters_input):
    try:
        st = time.time()
        rep = run_workflow(workflow, parameters_input)
        ed = time.time()
        if rep.get('failed', False):
            print(f"Request {request_id} failed for workflow {workflow}.")
            return
        ids[request_id] = {'time': ed - st, 'st': st, 'ed': ed, 'e2e_latency': rep['e2e_latency'], 'rounds': rep['rounds']}
        latencies.append(rep['e2e_latency'])
        firing_timestamps.append(st)
    except Exception as e:
        print(f"Error in post_request for workflow {workflow}: {e}")


def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    transaction_id = parameters.pop('transaction_id', None)
    if transaction_id:
        inputs['transaction_id'] = transaction_id
    try:
        # 设置 60 秒超时，防止请求无限期挂起
        rep = requests.post(url, json = inputs, timeout=60)
        rep.raise_for_status() # 检查 HTTP 错误
        return rep.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return {'e2e_latency': 0, 'rounds': 0, 'failed': True}

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    transaction_id = rep.get('transaction_id', '')
    return transaction_id, {
        "e2e_latency": rep.get('e2e_latency', 0),
        'rounds': rep.get('rounds', 0)
    }, rep['status']

def save_checkpoint(filepath, workflow_name, trace_id, start_idx, exp_duration):
    """保存当前实验数据的 Checkpoint"""
    print(f"\n[Checkpoint] Saving data to {filepath}...")
    save_logs = {
        'workflow_name': workflow_name,
        'trace_id': trace_id,
        'start_idx': start_idx,
        'exp_duration': exp_duration,
        'latencies': latencies,
        'firing_timestamps': firing_timestamps,
        'ids': ids
    }
    # 1. 保存带时间戳的备份文件 (保留历史)
    timestamp = int(time.time())
    backup_path = f"{filepath}.{timestamp}.tmp"
    with open(backup_path, 'w') as f:
        json.dump(save_logs, f)
        
    # 2. 更新主文件 (原子操作)
    import shutil
    temp_path = filepath + '.tmp'
    shutil.copy(backup_path, temp_path)
    os.rename(temp_path, filepath)

def checkpoint_loop(filepath, function_name, trace_id, start_idx, exp_duration):
    """每5分钟执行一次保存"""
    while True:
        gevent.sleep(300) # 300秒 = 5分钟
        save_checkpoint(filepath, function_name, trace_id, start_idx, exp_duration)


def run():
    # Trace #1 的配置
    trace_id = 1
    start_idx = 105674
    # end_idx = 154111
    exp_duration = 3600
   
    
    print(f'firing {workflow} with_trace {trace_id}-{start_idx} duration {exp_duration}s')

    # 准备时间戳 (逻辑参考 test.py)
    # 注意：这里假设 raw_trace 结构与 test.py 中使用的一致
    incoming_timestamps = raw_trace['per_function_invocations'][trace_id]['incoming_timestamps'][start_idx:]
    
    start_timestamp = incoming_timestamps[0] - 1
    last_timestamp = incoming_timestamps[0] + exp_duration

    # 截取实验时长内的时间戳
    valid_timestamps = []
    for ts in incoming_timestamps:
        if ts > last_timestamp:
            break
        valid_timestamps.append(ts)
    incoming_timestamps = valid_timestamps

    print(f'total request in trace: {len(incoming_timestamps)}')

    print(f"Generating parameters for {len(incoming_timestamps)} requests...")
    all_parameters = generate_param.generate_workflow_inputs_for_clients(workflow, 1, len(incoming_timestamps))[0]


    # 准备结果文件路径
    if not os.path.exists('result'):
        os.mkdir('result')
    
    filename = f"{mode}_Trace_{workflow}_{trace_id}_{start_idx}.json"
    filepath = os.path.join('result', filename)

    # 启动 Checkpoint 协程
    cp_greenlet = gevent.spawn(checkpoint_loop, filepath, workflow, trace_id, start_idx, exp_duration)
    start_local_time = time.time()
    request_idx = 0
    
    # 限制最大并发数为 10000，防止内存溢出
    pool = Pool(10000)

    # 主请求循环
    for i, time_stamp in enumerate(tqdm(incoming_timestamps)):
        delay = max(0, time_stamp - (start_timestamp + time.time() - start_local_time))
        gevent.sleep(delay)
        
        req_id = 'request_' + str(request_idx).rjust(5, '0')
        # 获取预生成的参数
        params = all_parameters[i]
        
        # 使用 pool.spawn 替代 gevent.spawn
        # 如果并发数达到 1000，这里会阻塞，直到有请求完成
        pool.spawn(post_request, workflow, req_id, params)
        request_idx += 1

    # 等待所有剩余请求完成
    pool.join()
    
    # 等待实验结束
    gevent.sleep(max(0, last_timestamp - (start_timestamp + time.time() - start_local_time)))
    gevent.sleep(15)

    # 停止 Checkpoint 协程
    cp_greenlet.kill()

    # 保存最终结果
    save_checkpoint(filepath, workflow, trace_id, start_idx, exp_duration)
    
    print('total requests count:', len(latencies))
    if latencies:
        print('avg:', format(sum(latencies) / len(latencies), '.3f'))

if __name__ == '__main__':
    run()