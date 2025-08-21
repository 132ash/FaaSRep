import boto3
import logging
import json
import sys

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

import pandas as pd
import multiprocessing
import requests
import config.config as config
from experiment.common import repository, client_logs
from experiment.common import generate_param
repo = repository.Repository()


DB_NODE_IP = config.STORAGE_NODE_IP
dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')
# table_name = f"{transaction_id}_shadow_table"

# travel_reservation_config
FLIGHT_IDS = config.FLIGHT_IDS
FLIGHT_CAPACITY = config.FLIGHT_CAPACITY
RENTAL_START = config.RENTAL_START
RENTAL_END = config.RENTAL_END
DATE_FORMAT = config.DATE_FORMAT

CLIENT_CNT = 16
ROUND = 100
parameters_inputs = {}
# all_workflows = ['banking_system']
all_workflows = ['social_network']
result_dict = {}


def worker_task(client_id, workflow, parameters_all_round, result_queue):
    """子进程执行的任务。"""
    client_logs.setup_logging_for_process(script_dir, client_id)
    # logging.info(f"Process started for workflow: {workflow}")
    
    local_results = []
    for i in range(ROUND):
        # 注意：analyze_workflow 需要能被子进程调用，并且其内部逻辑是进程安全的
        # 这里假设 analyze_workflow 返回一个包含结果的字典
        txid, result, tx_status = analyze_workflow(workflow, parameters_all_round[i])
        if tx_status == 'aborted':
            logging.info(f"[{client_id}] Round {i+1}/{ROUND} aborted for workflow {workflow}")
            continue
        local_results.append(result)
        # if i % 10 == 0:
        #     logging.info(f"[{client_id}] Round {i+1}/{ROUND} completed for workflow {workflow}, txid: {txid}, e2e_latency: {result['e2e_latency']}")
        # # logging.info(f"[{txid}] Finished, tx_res: {tx_res}")

    result_queue.put(local_results)
    logging.info("Process finished.")

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    transaction_id = parameters.pop('transaction_id', None)
    if transaction_id:
        inputs['transaction_id'] = transaction_id
    rep = requests.post(url, json = inputs)
    return rep.json()

def analyze_workflow(workflow, parameters_input):
    rep = run_workflow(workflow, parameters_input)
    # 直接返回包含所有需要信息的字典
    return {
        "transaction_id": rep.get('transaction_id', ''),
        "e2e_latency": rep.get('e2e_latency', 0),
        "workflow_exec_latency": rep.get('workflow_exec_latency', 0),
        "commit_latency": rep.get('commit_latency', 0),
        "status": rep.get('status', 'aborted')
    }

def analyze_all(system_mode="beldi", compute_mode='avg'):
    repo.flush_couchdb_workflow_latency()
    for workflow in all_workflows:
        parameters_all = generate_param.generate_workflow_inputs_for_clients(workflow, CLIENT_CNT, ROUND)
        result_queue = multiprocessing.Queue()
        processes = []
        for i in range(CLIENT_CNT):
            process = multiprocessing.Process(
                target=worker_task, 
                args=(i, workflow, parameters_all[i], result_queue)
            )
            processes.append(process)
        
        for p in processes:
            p.start()

        # 先从队列获取结果，避免死锁
        all_results = []
        for _ in range(CLIENT_CNT):
            all_results.extend(result_queue.get())

        # 然后等待所有进程结束
        for p in processes:
            p.join()

        if not all_results:
            print(f"No results collected for workflow {workflow}. Skipping.")
            continue

        # 1. 将网关返回的结果转换为DataFrame
        gateway_df = pd.DataFrame(all_results)
        successful_txs = gateway_df[gateway_df['status'] == 'success']
        if successful_txs.empty:
            print(f"No successful transactions for workflow {workflow}. Skipping analysis.")
            continue
        
        txids = successful_txs['transaction_id'].tolist()

        # 2. 批量从Repo获取详细延迟
        repo_latencies = repo.get_all_latencies_for_txs(txids)
        repo_df = pd.DataFrame.from_dict(repo_latencies, orient='index').reset_index().rename(columns={'index': 'transaction_id'})

        # 3. 合并两个DataFrame
        df = pd.merge(successful_txs, repo_df, on='transaction_id', how='left').fillna(0)

        # 4. 计算派生延迟
        df['function_exec_latency'] = df['exec_latency'] - df['io_latency']
        df['scheduling_latency'] = df['workflow_exec_latency'] - df['exec_latency']
        
        # 5. 统计所需指标的平均值
        avg_latency = df.mean()
        summary = {
            "mode": f"{system_mode}_{compute_mode}",
            "e2e_latency": avg_latency.get("e2e_latency"),
            "scheduling_latency": avg_latency.get("scheduling_latency"),
            "lock_latency": avg_latency.get("lock_latency"),
            "io_latency": avg_latency.get("io_latency"),
            "function_exec_latency": avg_latency.get("function_exec_latency"),
            "commit_latency": avg_latency.get("commit_latency")
        }

        summary_df = pd.DataFrame([summary])
        output_file = script_dir / 'results' / f"{workflow}_{summary['mode']}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(output_file, index=False)
        
        print(f"[{workflow}] Results summary saved to {output_file}")
        print(summary_df)


if __name__ == '__main__':
    # 简化了main函数的参数处理，使其更清晰
    system_mode_arg = sys.argv[1] if len(sys.argv) > 1 else 'beldi'
    compute_mode_arg = sys.argv[3] if len(sys.argv) > 3 else 'avg'
    analyze_all(system_mode=system_mode_arg, compute_mode=compute_mode_arg)