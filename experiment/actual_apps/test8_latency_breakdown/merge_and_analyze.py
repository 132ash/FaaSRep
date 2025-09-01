import pandas as pd
import sys
from pathlib import Path

def main():
    """
    从预先保存的CSV文件中加载数据，合并它们，执行延迟分析，并保存最终的摘要结果。
    """
    # 1. 从命令行获取参数
    if len(sys.argv) != 3:
        print("用法: python merge_and_analyze.py <workflow_name> <system_mode>")
        print("示例: python merge_and_analyze.py travel_reservation beldi")
        sys.exit(1)

    workflow_name = sys.argv[1]
    system_mode = sys.argv[2]
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / 'results'
    # 2. 定义并加载输入文件
    gateway_results_path = results_dir / f"travel_reservation_beldi_gateway_results.csv"
    repo_latencies_path = results_dir / f"all_latencies_travel_reservation.csv"

    print(f"Loading gateway data from: {gateway_results_path}")
    print(f"Loading repository data from: {repo_latencies_path}")

    try:
        gateway_df = pd.read_csv(gateway_results_path)
        # repo_latencies.csv 可能不存在（如果没有从数据库获取到数据），需要处理这种情况
        if repo_latencies_path.exists():
            repo_df = pd.read_csv(repo_latencies_path)
        else:
            print(f"Warning: Repository latencies file not found at {repo_latencies_path}. Breakdown latencies will be zero.")
            # 创建一个空的 DataFrame，但要包含 transaction_id 以便合并
            repo_df = pd.DataFrame(columns=['transaction_id', 'exec_latency', 'io_latency', 'lock_latency'])

    except FileNotFoundError as e:
        print(f"错误: 输入文件未找到 - {e}")
        sys.exit(1)

    # 3. 合并两个DataFrame (与 run.py 中的逻辑相同)
    # 注意：这里我们直接使用 gateway_df，因为它已经包含了所有成功的事务
    df = pd.merge(gateway_df, repo_df, on='transaction_id', how='left').fillna(0)

    # 4. 计算派生延迟 (与 run.py 中的逻辑相同)
    # 确保在计算前，'lock_latency' 列存在
    if 'lock_latency' not in df.columns:
        df['lock_latency'] = 0
        
    df['function_exec_latency'] = df['exec_latency'] - df['io_latency'] - df['lock_latency']
    df['scheduling_latency'] = df['workflow_exec_latency'] - df['exec_latency']
    
    # 确保 'rounds' 列存在
    if 'rounds' not in df.columns:
        df['rounds'] = 1 # 如果原始数据中没有，则默认为1
        
    p99_rounds_val = df['rounds'].quantile(0.99)

    # 5. 统计所需指标的平均值 (与 run.py 中的逻辑相同)
    numeric_columns = [
        'e2e_latency', 'workflow_exec_latency', 'commit_latency', 'rounds',
        'exec_latency', 'io_latency', 'lock_latency', 
        'function_exec_latency', 'scheduling_latency'
    ]
    # 确保所有要计算的列都存在于DataFrame中
    existing_numeric_columns = [col for col in numeric_columns if col in df.columns]
    avg_latency = df[existing_numeric_columns].mean()

    summary = {
        "mode": f"{system_mode}",
        "e2e_latency": avg_latency.get("e2e_latency", 0),
        'workflow_exec_latency': avg_latency.get("workflow_exec_latency", 0),
        "scheduling_latency": avg_latency.get("scheduling_latency", 0),
        'func_e2e_latency': avg_latency.get("exec_latency", 0),
        "lock_latency": avg_latency.get("lock_latency", 0),
        "io_latency": avg_latency.get("io_latency", 0),
        "function_exec_latency": avg_latency.get("function_exec_latency", 0),
        "commit_latency": avg_latency.get("commit_latency", 0),
        "avg_rounds": avg_latency.get("rounds", 0),
        "p99_rounds": p99_rounds_val
    }

    # 6. 保存最终的摘要文件
    summary_df = pd.DataFrame([summary])
    output_file = results_dir / f"{workflow_name}_{summary['mode']}_summary.csv"
    summary_df.to_csv(output_file, index=False)
    
    print("\n--- Analysis Complete ---")
    print(f"Final summary saved to: {output_file}")
    print(summary_df.to_string(index=False))

if __name__ == '__main__':
    main()
