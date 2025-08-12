#!/usr/bin/env python3
# filepath: /home/shao/FaaSnap/experiment/microbenchmark/test1:Latency&Throughput/process_results.py

import sys
import pandas as pd
from pathlib import Path

def process_workflow_results(workflow, temp_file):
    """处理单个工作流的结果并生成最终CSV文件"""
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / "results"
    results_dir.mkdir(exist_ok=True)
    
    try:
        # 读取临时结果文件
        df = pd.read_csv(temp_file)
        
        if df.empty:
            print(f"错误: {workflow} 的结果文件为空")
            return False
        
        # 按客户端数量排序
        df = df.sort_values('client_count')
        
   
        # 生成最终结果文件
        output_file = results_dir / f"{workflow}_res.csv"
        df.to_csv(output_file, index=False)
        
        print(f"✅ {workflow} 结果已保存到: {output_file}")
        return True
        
    except Exception as e:
        print(f"错误: 处理 {workflow} 结果时出错: {str(e)}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("用法: python3 process_results.py <WORKFLOW> <TEMP_FILE>")
        sys.exit(1)
    
    workflow = sys.argv[1]
    temp_file = sys.argv[2]
    
    success = process_workflow_results(workflow, temp_file)
    sys.exit(0 if success else 1)