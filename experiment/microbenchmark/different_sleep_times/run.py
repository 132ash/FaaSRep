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

ROUND = 50
TEXT_SIZE = 4 * 1024
parameters_inputs = {}
result_dict = {}

def setup_logging():
    """设置日志配置，确保实时输出到终端"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    # 确保立即刷新输出
    logging.getLogger().handlers[0].flush = lambda: sys.stdout.flush()

def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程执行的任务。"""
    local_results = []
    batch_size = 50  # 每50个结果发送一次，避免队列阻塞
    
    for i in range(ROUND):
        txid, result = analyze_workflow(workflow, parameters_all_round[i])
        if not txid:
            print(f"Client {client_id}: No result for round {i}, skipping.", flush=True)
            continue
        result['client_id'] = client_id
        result['round'] = i + 1
        
        local_results.append(result)
        
        # 每batch_size个结果或最后一轮时发送到队列
        if (i + 1) % batch_size == 0 or i == ROUND - 1:
            try:
                # 发送当前批次的结果
                result_queue.put((client_id, local_results), timeout=5)
                local_results = []  # 清空本地结果列表
                
            except Exception as e:
                print(f"Client {client_id}: Error putting results to queue: {e}", flush=True)
                break
        if i % (max(ROUND // 10,1)) == 0 or i == ROUND - 1:
            print(f"Client {client_id}: Round {i} completed, batch sent", flush=True)
        
    
    # 发送结束信号
    try:
        result_queue.put((client_id, 'DONE'), timeout=5)
        print(f"Client {client_id}: All tasks completed and results sent", flush=True)
    except Exception as e:
        print(f"Client {client_id}: Error sending completion signal: {e}", flush=True)

def result_collector_thread(result_queue, all_results, client_cnt, stop_event):
    """在单独线程中收集结果，避免队列阻塞"""
    completed_clients = set()
    
    while len(completed_clients) < client_cnt and not stop_event.is_set():
        try:
            client_id, data = result_queue.get(timeout=1)
            
            if data == 'DONE':
                completed_clients.add(client_id)
                print(f"📦 客户端 {client_id} 完成所有任务", flush=True)
            else:
                # data 是结果列表
                all_results.extend(data)
                
        except Exception as e:
            # 超时或其他错误，继续等待
            continue
    
    print(f"📊 结果收集完成，共收集 {len(all_results)} 个结果", flush=True)

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    rep = requests.post(url, json = inputs)
    if not rep:
        return {}
    return rep.json()

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    if not rep:
        return None, {}
    return rep['transaction_id'], {
        "e2e_latency": rep['e2e_latency'],
    }

def write_result_to_file(system_mode, workflow_name, client_cnt, median_latency, p99_latency, avg_latency, avg_throughput, sleep_time):
    """将汇总结果追加到最终结果文件"""
    # 结果保存到 results/beldi/summary_results.csv
    mode_dir = script_dir / "results" / system_mode
    mode_dir.mkdir(parents=True, exist_ok=True)
    
    result_file = mode_dir / "summary_results.csv"
    
    # 检查文件是否存在，如果不存在则创建并写入表头
    # 表头只包含 sleep_time 和性能指标
    if not result_file.exists():
        with open(result_file, 'w') as f:
            f.write("workflow_name,client_cnt,sleep_time,median_latency,p99_latency,avg_latency,avg_throughput\n")
        print(f"📝 创建汇总结果文件: {result_file}", flush=True)
    
    # 追加结果数据
    with open(result_file, 'a') as f:
        f.write(f"{workflow_name},{client_cnt},{sleep_time},{median_latency:.4f},{p99_latency:.4f},{avg_latency:.4f},{avg_throughput:.4f}\n")
    
    print(f"📊 汇总结果已写入文件: {result_file}", flush=True)
    print(f"📊 数据: Sleep={sleep_time}, P50={median_latency:.4f}, P99={p99_latency:.4f}, Avg={avg_latency:.4f}, TPS={avg_throughput:.4f}", flush=True)

def analyze_all(workflow_name, system_mode, client_cnt, sleep_time):
    print(f"🚀 开始测试 - 工作流: {workflow_name}, 模式: {system_mode}, 客户端: {client_cnt}, Sleep: {sleep_time}", flush=True)
    
    # --- 设置结果目录 ---
    mode_dir = script_dir / "results" / system_mode
    raw_results_dir = mode_dir / "raw_results"
    mode_dir.mkdir(parents=True, exist_ok=True)
    raw_results_dir.mkdir(exist_ok=True)
    # --- 结束目录设置 ---

    sys.stdout.flush()  # 强制刷新输出缓冲区
    repo.flush_couchdb_workflow_latency()
    #repo.clear_all_memory_and_container()
    
    # 生成参数 (注意：这里不注入 sleep_time 到 parameters_all 中)
    parameters_all = generate_param.generate_workflow_inputs_for_clients('microbenchmark', client_cnt, ROUND, workflow_name, 0.9)
    print("Parameters ready.")
    
    # 使用更大的队列或无限大小队列
    result_queue = multiprocessing.Queue(maxsize=1000) # 设置较大的队列大小
    
    # 用于存储所有结果的列表（线程安全）
    all_results = []
    stop_event = threading.Event()
    
    # 启动结果收集线程
    collector_thread = threading.Thread(
        target=result_collector_thread,
        args=(result_queue, all_results, client_cnt, stop_event)
    )
    collector_thread.daemon = True
    collector_thread.start()
    
    # 创建client_cnt个进程
    processes = []
    for i in range(client_cnt):
        process = multiprocessing.Process(
            target=worker_task, 
            args=(i, workflow_name, parameters_all[i], result_queue)
        )
        processes.append(process)
    
    print(f"📋 启动 {client_cnt} 个客户端进程...", flush=True)
    start_time = time.time()
    for i in range(client_cnt):
        processes[i].start()
        print(f"✅ 启动进程 {processes[i].pid} (客户端 {i})", flush=True)

    print(f"⏳ 等待所有进程完成...", flush=True)
    
    # 等待所有子进程运行结束
    for i, process in enumerate(processes):
        process.join() 
        print(f"✅ 进程 {i} 完成", flush=True)

    # 等待结果收集完成
    print(f"📊 等待结果收集完成...", flush=True)
    collector_thread.join(timeout=30)  # 最多等待30秒收集结果
    stop_event.set()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"📊 收集测试结果... 总计用时: {total_time:.2f} 秒", flush=True)

    # 统计结果
    if not all_results:
        print(f"❌ 错误: 工作流 {workflow_name} 没有收集到任何结果", flush=True)
        sys.exit(1)
        
    df = pd.DataFrame(all_results)
    
    # --- 保存原始结果 ---
    raw_result_filename = raw_results_dir / f"{workflow_name}_{client_cnt}_sleep_{sleep_time}_raw.csv"
    df.to_csv(raw_result_filename, index=False)
    print(f"📝 原始结果已保存到: {raw_result_filename}", flush=True)
    # --- 结束保存 ---
    
    median_e2e_latency = df['e2e_latency'].quantile(0.50)
    p99_e2e_latency = df['e2e_latency'].quantile(0.99) # P99延迟
    avg_e2e_latency = df['e2e_latency'].mean()         # 平均延迟
    
    # 计算平均吞吐量: client_count / 平均延迟
    avg_throughput = client_cnt / avg_e2e_latency  # 转换为 RPS (延迟单位是s)
    
    print(f"   总测试时间: {total_time:.2f} 秒", flush=True)
    print(f"   中位数 E2E 延迟: {median_e2e_latency:.4f} s", flush=True)
    print(f"   P99 E2E 延迟: {p99_e2e_latency:.4f} s", flush=True)
    print(f"   平均 E2E 延迟: {avg_e2e_latency:.4f} s", flush=True)
    print(f"   平均吞吐量: {avg_throughput:.4f} RPS", flush=True)

    # 直接写入结果文件
    write_result_to_file(system_mode, workflow_name, client_cnt, median_e2e_latency, p99_e2e_latency, avg_e2e_latency, avg_throughput, sleep_time)
    
    print(f"✅ {workflow_name} 测试完成 (客户端: {client_cnt}, Sleep: {sleep_time})", flush=True)
    sys.stdout.flush()  # 确保所有输出都被刷新
    
    return median_e2e_latency, avg_throughput

if __name__ == '__main__':
    # 设置日志配置
    setup_logging()
    
    if len(sys.argv) != 4:
        print("用法: python run.py <workflow_name> <client_count> <sleep_time>", flush=True)
        sys.exit(1)
        
        
    workflow_name = sys.argv[1]
    client_cnt = int(sys.argv[2])
    sleep_time = sys.argv[3] # 保持为字符串，用于文件名和CSV记录
    system_mode = 'beldi'
    
    
    try:
        analyze_all(workflow_name, system_mode, client_cnt, sleep_time)
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)