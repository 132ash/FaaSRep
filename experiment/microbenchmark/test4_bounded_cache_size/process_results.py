#!/usr/bin/env python3
# filepath: /home/shao/FaaSnap/experiment/microbenchmark/test1:Latency&Throughput/process_results.py

import sys
import pandas as pd
from pathlib import Path

def process_mode_results(system_mode):
    """整理单个模式的结果文件：排序"""
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / "results"
    results_dir.mkdir(exist_ok=True)
    
    result_file = results_dir / f"{system_mode}_res.csv"
    
    if not result_file.exists():
        print(f"错误: {system_mode} 的结果文件不存在: {result_file}")
        return False
    
    try:
        # 读取结果文件
        df = pd.read_csv(result_file)
        
        if df.empty:
            print(f"错误: {system_mode} 的结果文件为空")
            return False
        
        # 按cache_bound排序
        df_sorted = df.sort_values('cache_bound')
        
        # 保存整理后的结果
        df_sorted.to_csv(result_file, index=False)
        
        print(f"✅ {system_mode} 结果已整理完成: {result_file}")
        return True
        
    except Exception as e:
        print(f"错误: 处理 {system_mode} 结果时出错: {str(e)}")
        return False

def show_mode_results(system_mode):
    """显示单个模式的结果"""
    
    script_dir = Path(__file__).parent
    results_dir = script_dir / "results"
    result_file = results_dir / f"{system_mode}_res.csv"
    
    if not result_file.exists():
        print(f"错误: {system_mode} 的结果文件不存在: {result_file}")
        return False
    
    try:
        df = pd.read_csv(result_file)
        print(f"\n📊 {system_mode} 测试结果:")
        print(df.to_string(index=False))
        return True
        
    except Exception as e:
        print(f"错误: 读取 {system_mode} 结果时出错: {str(e)}")
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法:")
        print("  整理结果: python3 process_results.py <SYSTEM_MODE>")
        print("  显示结果: python3 process_results.py <SYSTEM_MODE> --show")
        sys.exit(1)
    
    system_mode = sys.argv[1]
    
    if len(sys.argv) > 2 and sys.argv[2] == '--show':
        success = show_mode_results(system_mode)
    else:
        success = process_mode_results(system_mode)
    
    sys.exit(0 if success else 1)