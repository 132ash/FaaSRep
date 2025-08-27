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
import os
import time

def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

script_dir = Path(__file__).parent
ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

from config import config
from experiment.common import repository, generate_param
repo = repository.Repository()

DB_NODE_IP = config.STORAGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
table_name = "data"
table = dynamodb.Table(table_name)

ROUND = 100
TEXT_SIZE = 4 * 1024
parameters_inputs = {}
result_dict = {}

def setup_logging():
    """设置日志配置，将日志输出到 stderr。"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr) # 关键修改：输出到 stderr
        ],
        force=True
    )

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程执行的任务。"""
    local_results = []
    batch_size = 50
    
    for i in range(ROUND):
        txid, result = analyze_workflow(workflow, parameters_all_round[i])
        result['client_id'] = client_id
        result['round'] = i + 1
        local_results.append(result)
        
        if (i + 1) % batch_size == 0 or i == ROUND - 1:
            try:
                result_queue.put((client_id, local_results), timeout=5)
                local_results = []
            except Exception as e:
                logging.error(f"Client {client_id}: Error putting results to queue: {e}")
                break
        if i % (ROUND // 10) == 0:
            logging.info(f"Client {client_id}: Completed {i + 1} rounds")

    try:
        result_queue.put((client_id, 'DONE'), timeout=5)
    except Exception as e:
        logging.error(f"Client {client_id}: Error sending completion signal: {e}")

def result_collector_thread(result_queue, all_results, client_cnt, stop_event):
    """在单独线程中收集结果。"""
    completed_clients = set()
    
    while len(completed_clients) < client_cnt and not stop_event.is_set():
        try:
            client_id, data = result_queue.get(timeout=1)
            if data == 'DONE':
                completed_clients.add(client_id)
            else:
                all_results.extend(data)
        except Exception:
            continue
    logging.info(f"结果收集完成，共收集 {len(all_results)} 个结果")

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    rep = requests.post(url, json = inputs)
    return rep.json()

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    return rep['transaction_id'], {
        "e2e_latency": rep['e2e_latency']
    }

def write_result_to_file(client_cnt, system_mode, remote_prob, median_latency, p99_latency, avg_throughput):
    """将汇总结果追加到最终结果文件。"""
    mode_dir = script_dir / "results" / system_mode
    mode_dir.mkdir(parents=True, exist_ok=True)
    
    result_file = mode_dir / "summary_results_by_prob.csv"
    
    # 检查文件是否存在，如果不存在则创建并写入表头
    if not result_file.exists():
        with open(result_file, 'w') as f:
            f.write("clients,remote_prob,median_latency,p99_latency,avg_throughput\n")
        logging.info(f"创建汇总结果文件: {result_file}")
    
    # 追加结果数据
    with open(result_file, 'a') as f:
        f.write(f"{client_cnt},{remote_prob},{median_latency:.4f},{p99_latency:.4f},{avg_throughput:.4f}\n")
    
    logging.info(f"汇总结果已写入文件: {result_file}")

def analyze_all(workflow_name, system_mode, client_cnt, zipf_param, remote_prob):
    logging.info(f"开始测试 - 工作流: {workflow_name}, 模式: {system_mode}, 客户端: {client_cnt}, zipf: {zipf_param:.2f}, 失效概率: {remote_prob}")

    # --- 设置结果目录 ---
    mode_dir = script_dir / "results" / system_mode
    raw_results_dir = mode_dir / "raw_results"
    mode_dir.mkdir(parents=True, exist_ok=True)
    raw_results_dir.mkdir(exist_ok=True)
    
    
    repo.flush_couchdb_workflow_latency()
    parameters_all = generate_param.generate_workflow_inputs_for_clients('microbenchmark', client_cnt, ROUND, micro_workflow=workflow_name, zipf_param=zipf_param)
    result_queue = multiprocessing.Queue(maxsize=1000)
    
    all_results = []
    stop_event = threading.Event()
    
    collector_thread = threading.Thread(
        target=result_collector_thread,
        args=(result_queue, all_results, client_cnt, stop_event)
    )
    collector_thread.daemon = True
    collector_thread.start()
    
    processes = [multiprocessing.Process(target=worker_task, args=(i, workflow_name, parameters_all[i], result_queue)) for i in range(client_cnt)]
    
    logging.info(f"启动 {client_cnt} 个客户端进程...")
    start_time = time.time()
    for p in processes:
        p.start()
    
    for p in processes:
        p.join() 

    collector_thread.join(timeout=30)
    stop_event.set()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    logging.info(f"测试完成，收集结果... 总计用时: {total_time:.2f} 秒")

    if not all_results:
        logging.error(f"工作流 {workflow_name} 没有收集到任何结果")
        return
        
    df = pd.DataFrame(all_results)
    raw_result_filename = raw_results_dir / f"{workflow_name}_{client_cnt}_{remote_prob}_raw.csv"
    df.to_csv(raw_result_filename, index=False)
    logging.info(f"原始结果已保存到: {raw_result_filename}")
    
    median_e2e_latency = df['e2e_latency'].quantile(0.50)
    p99_e2e_latency = df['e2e_latency'].quantile(0.99)
    avg_throughput = (client_cnt * ROUND) / total_time
    
    logging.info(f"测试结果 (失效概率: {remote_prob}): 中位数延迟={median_e2e_latency:.4f}s, P99延迟={p99_e2e_latency:.4f}s, 吞吐量={avg_throughput:.4f} RPS")

    write_result_to_file(client_cnt, system_mode, remote_prob, median_e2e_latency, p99_e2e_latency, avg_throughput)

if __name__ == '__main__':
    setup_logging()
    
    if len(sys.argv) != 6:
        logging.error("用法: python run.py <workflow_name> <system_mode> <client_count> <zipf_param> <remote_prob>")
        sys.exit(1)
        
    workflow_name = sys.argv[1]
    system_mode = sys.argv[2]
    client_cnt = int(sys.argv[3])
    zipf_param = float(sys.argv[4])
    remote_prob = float(sys.argv[5])
    
    try:
        analyze_all(workflow_name, system_mode, client_cnt, zipf_param, remote_prob)
    except Exception as e:
        logging.error(f"测试过程中发生严重错误: {e}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)