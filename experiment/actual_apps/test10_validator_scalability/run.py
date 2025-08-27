import multiprocessing
import time
import requests
import sys
import os
import pandas as pd
from pathlib import Path
import uuid
import numpy as np

# --- 项目路径设置 ---
script_dir = Path(__file__).parent
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.insert(0, str(ROOT_DIR))

import config.config as config

# --- 测试配置 ---
# CLIENT_COUNTS = [32, 64, 128, 256, 512]
CLIENT_COUNTS = [80]
REQUESTS_PER_CLIENT = 100
BATCH_SIZE = 4
WORKFLOW_NAME = 'c4'


# --- 服务地址配置 (假定服务已在外部启动) ---
FAKE_GATEWAY_HOST = '127.0.0.1'
FAKE_GATEWAY_PORT = 8000
FAKE_GATEWAY_URL = config.FAKE_REQUEST_URL

# --- Zipf 分布和数据集配置 ---
DB_SIZE = 10000
ZIPF_ALPHA = 0.9
DATASET = [f'key_{i}' for i in range(DB_SIZE)]

class ZipfGenerator:
    def __init__(self, n, alpha):
        if alpha < 0: raise ValueError("alpha must be >= 0")
        self.n = n
        self.alpha = alpha
        if alpha == 0:
            self.probabilities = np.full(n, 1/n)
        else:
            weights = np.power(np.arange(1, n + 1, dtype=float), -alpha)
            self.probabilities = weights / np.sum(weights)
        self.cdf = np.cumsum(self.probabilities)

    def sample(self, k=1):
        random_values = np.random.rand(k)
        samples = np.searchsorted(self.cdf, random_values)
        return samples if k > 1 else samples[0]

zipf_sampler = ZipfGenerator(DB_SIZE, ZIPF_ALPHA)

def generate_fake_batch():
    """使用 Zipf 分布生成一个模拟的 c4 工作流批次数据"""
    transaction_list = [str(uuid.uuid4()) for _ in range(BATCH_SIZE)]
    read_set, write_set, container_port = {}, {}, {}
    for tx_id in transaction_list:
        read_indices = zipf_sampler.sample(k=8)
        write_indices = zipf_sampler.sample(k=4)
        tx_read_set = {DATASET[i]: 'v0' for i in read_indices}
        tx_write_set = {DATASET[i]: f'f{i%4+1}' for i in write_indices}
        read_set[tx_id] = {'f1': tx_read_set}
        write_set[tx_id] = tx_write_set
        container_port[tx_id] = {f'f{i}': 8000 + i for i in range(1, 5)}
    return {
        'workflow_name': WORKFLOW_NAME,
        'batch_id': str(uuid.uuid4()),
        'batch': {
            'transaction_list': transaction_list, 'read_set': read_set,
            'write_set': write_set, 'container_port': container_port,
            'RYW_subjection': {}
        }
    }

def client_task(result_queue):
    """每个客户端进程执行的任务"""
    session = requests.Session()
    successful_requests = 0
    for i in range(REQUESTS_PER_CLIENT):
        batch_data = generate_fake_batch()
        try:
            response = session.post(FAKE_GATEWAY_URL, json={'fake_batch': batch_data}, timeout=20)
            if response.status_code == 200:
                successful_requests += 1
            else:
                print(f"客户端收到错误响应: {response.status_code}", file=sys.stderr)
        except requests.exceptions.RequestException as e:
            print(f"客户端请求失败: {e}", file=sys.stderr)
            break
        if i % (REQUESTS_PER_CLIENT // 10) == 0 or i == REQUESTS_PER_CLIENT - 1:
            print(f"客户端进度: {i / REQUESTS_PER_CLIENT * 100:.1f}%", flush=True)
    result_queue.put(successful_requests)

def run_test(client_count):
    """运行单次可扩展性测试"""
    print(f"\n--- [进度] 开始测试: {client_count} 个并发客户端 ---", flush=True)
    result_queue = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(target=client_task, args=(result_queue,))
        for _ in range(client_count)
    ]

    print(f"--- [进度] 已创建 {client_count} 个客户端进程，正在启动...", flush=True)
    start_time = time.time()
    for p in processes:
        p.start()

    print(f"--- [进度] 所有进程已启动，等待其完成...", flush=True)
    for p in processes:
        p.join()
    end_time = time.time()
    print(f"--- [进度] 所有进程已结束，开始收集结果...", flush=True)

    total_successful_batches = 0
    while not result_queue.empty():
        total_successful_batches += result_queue.get()

    total_time = end_time - start_time
    total_transactions = total_successful_batches * BATCH_SIZE
    throughput = total_transactions / total_time if total_time > 0 else 0

    print(f"--- [结果] 测试完成. 总用时: {total_time:.2f}s", flush=True)
    print(f"--- [结果] 成功批次数: {total_successful_batches}", flush=True)
    print(f"--- [结果] 成功事务数: {total_transactions}", flush=True)
    print(f"--- [结果] 平均吞吐量: {throughput:.2f} txns/sec", flush=True)
    
    return throughput

def main():
    """主函数，负责编排客户端负载测试"""
    validator_workers = config.VALIDATORS_PER_POOL
    print(f"配置信息: Validator Worker 数量 = {validator_workers} (用于结果记录)", flush=True)
    print(f"测试目标: {FAKE_GATEWAY_URL}", flush=True)

    results_dir = script_dir / "results"
    results_dir.mkdir(exist_ok=True)
    summary_file = results_dir / f"validator_scalability_summary_w{validator_workers}.csv"

    all_results = []
    try:
        # 循环测试不同的客户端并发数
        for count in CLIENT_COUNTS:
            throughput = run_test(client_count=count)
            all_results.append({
                'validator_workers': validator_workers,
                'client_count': count, 
                'throughput_txns_per_sec': throughput
            })
    
    except Exception as e:
        print(f"测试期间发生严重错误: {e}", file=sys.stderr)
    
    # 保存结果
    if all_results:
        df = pd.DataFrame(all_results)
        # 如果文件不存在，则创建并写入表头
        if not summary_file.exists():
            df.to_csv(summary_file, index=False, mode='w')
        else: # 否则以追加模式写入，不写表头
            df.to_csv(summary_file, index=False, mode='a', header=False)
            
        print(f"\n测试结果已保存到: {summary_file}")
        print(df.to_string(index=False))

if __name__ == '__main__':
    # 在 macOS 和 Windows 上，'fork' 可能不是默认或可用的，'spawn' 或 'forkserver' 更安全
    # 但对于 Linux 上的性能测试，'fork' 通常是最高效的
    if sys.platform != 'win32':
        multiprocessing.set_start_method('fork', force=True)
    main()