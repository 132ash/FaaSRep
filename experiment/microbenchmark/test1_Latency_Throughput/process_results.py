#!/usr/bin/env python3
# filepath: /home/shao/FaaSnap/experiment/microbenchmark/test1:Latency&Throughput/process_results.py

import sys
import pandas as pd
from pathlib import Path

def process_workflow_results(workflow):
    """整理单个工作流的结果文件：排序、去重"""
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / "results"
    results_dir.mkdir(exist_ok=True)
    
    result_file = results_dir / f"{workflow}_res.csv"
    
    if not result_file.exists():
        print(f"错误: {workflow} 的结果文件不存在: {result_file}")
        return False
    
    try:
        # 读取结果文件
        df = pd.read_csv(result_file)
        
        if df.empty:
            print(f"错误: {workflow} 的结果文件为空")
            return False
        
        # 去重：如果有相同的workflow和client_count，保留最新的（最后一条）
        df_cleaned = df.drop_duplicates(subset=['workflow', 'client_count'], keep='last')
        
        # 按客户端数量排序
        df_cleaned = df_cleaned.sort_values('client_count')
        
        # 保存整理后的结果
        df_cleaned.to_csv(result_file, index=False)
        
        print(f"✅ {workflow} 结果已整理完成: {result_file}")
        print(f"   原始记录数: {len(df)}")
        print(f"   整理后记录数: {len(df_cleaned)}")
        return True
        
    except Exception as e:
        print(f"错误: 处理 {workflow} 结果时出错: {str(e)}")
        return False

def show_workflow_results(workflow):
    """显示单个工作流的结果"""
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / "results"
    result_file = results_dir / f"{workflow}_res.csv"
    
    if not result_file.exists():
        print(f"错误: {workflow} 的结果文件不存在: {result_file}")
        return False
    
    try:
        df = pd.read_csv(result_file)
        print(f"\n📊 {workflow} 测试结果:")
        print(df.to_string(index=False))
        return True
        
    except Exception as e:
        print(f"错误: 读取 {workflow} 结果时出错: {str(e)}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法:")
        print("  整理结果: python3 process_results.py <WORKFLOW>")
        print("  显示结果: python3 process_results.py <WORKFLOW> --show")
        sys.exit(1)
    
    workflow = sys.argv[1]
    
    if len(sys.argv) > 2 and sys.argv[2] == '--show':
        success = show_workflow_results(workflow)
    else:
        success = process_workflow_results(workflow)
    
    sys.exit(0 if success else 1)