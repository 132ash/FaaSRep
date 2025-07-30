#!/usr/bin/env python3
"""
生成微基准测试配置文件的脚本
为n=[2,4,8,16]生成cn文件夹（链式）和wn文件夹（并行分支），包含function_info.yaml和workflow.yaml文件
"""

import os
import yaml
from pathlib import Path


def generate_function_info_chain(n):
    """生成链式workflow的function_info.yaml内容"""
    function_info = {
        'workflow': f"c{n}",
        'functions': [],
        'max_containers': 30
    }
    
    # 生成从f1到fn的函数列表
    for i in range(1, n + 1):
        function_info['functions'].append({
            'image': 'micro_func',
            'name': f'f{i}'
        })
    
    return function_info


def generate_function_info_parallel(n):
    """生成并行workflow的function_info.yaml内容"""
    function_info = {
        'workflow': f'w{n}',
        'functions': [],
        'max_containers': 30
    }
    
    # f1函数
    function_info['functions'].append({
        'image': 'micro_func',
        'name': 'f1'
    })
    
    # f2_1 到 f2_(n-1) 并行函数
    for i in range(1, n):
        function_info['functions'].append({
            'image': 'micro_func',
            'name': f'f2_{i}'
        })
    
    # f3汇聚函数
    function_info['functions'].append({
        'image': 'micro_func',
        'name': 'f3'
    })
    
    return function_info


def generate_workflow_chain(n):
    """生成链式workflow.yaml内容"""
    workflow = {
        'functions': []
    }
    
    # 生成从f1到fn的函数链
    for i in range(1, n + 1):
        function_def = {
            'name': f'f{i}',
            'source': f'f{i}',
            'input': {
                'keys': {
                    'from': 'GLOBAL',
                    'type': 'str'
                },
                'payload_size': {
                    'from': 'GLOBAL',
                    'type': 'int'
                }
            }
        }
        
        # 设置next节点
        if i < n:  # 不是最后一个函数
            function_def['next'] = {
                'type': 'pass',
                'nodes': [f'f{i + 1}']
            }
        else:  # 最后一个函数
            function_def['next'] = {
                'type': 'FINISH'
            }
        
        workflow['functions'].append(function_def)
    
    return workflow


def generate_workflow_parallel(n):
    """生成并行workflow.yaml内容 - f1分支到n-1个并行函数，然后汇聚到f3"""
    workflow = {
        'functions': []
    }
    
    # f1函数 - 分支到所有f2_x函数
    f1_nodes = [f'f2_{i}' for i in range(1, n)]
    f1_def = {
        'name': 'f1',
        'source': 'f1',
        'input': {
            'keys': {
                'from': 'GLOBAL',
                'type': 'str'
            },
            'payload_size': {
                'from': 'GLOBAL',
                'type': 'int'
            }
        },
        'next': {
            'type': 'pass',
            'nodes': f1_nodes
        }
    }
    workflow['functions'].append(f1_def)
    
    # f2_1 到 f2_(n-1) 并行函数
    for i in range(1, n):
        f2_def = {
            'name': f'f2_{i}',
            'source': f'f2_{i}',
            'input': {
                'keys': {
                    'from': 'GLOBAL',
                    'type': 'str'
                },
                'payload_size': {
                    'from': 'GLOBAL',
                    'type': 'int'
                }
            },
            'next': {
                'type': 'pass',
                'nodes': ['f3']
            }
        }
        workflow['functions'].append(f2_def)
    
    # f3汇聚函数
    f3_def = {
        'name': 'f3',
        'source': 'f3',
        'input': {
            'keys': {
                'from': 'GLOBAL',
                'type': 'str'
            },
            'payload_size': {
                'from': 'GLOBAL',
                'type': 'int'
            }
        },
        'next': {
            'type': 'FINISH'
        }
    }
    workflow['functions'].append(f3_def)
    
    return workflow


def create_config_folder_chain(n, base_path):
    """为给定的n创建Cn文件夹和配置文件（链式结构）"""
    folder_name = f'c{n}'
    folder_path = base_path / folder_name
    
    # 创建文件夹
    folder_path.mkdir(exist_ok=True)
    print(f"创建文件夹: {folder_path}")
    
    # 生成并写入function_info.yaml
    function_info = generate_function_info_chain(n)
    function_info_path = folder_path / 'function_info.yaml'
    with open(function_info_path, 'w', encoding='utf-8') as f:
        yaml.dump(function_info, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {function_info_path}")
    
    # 生成并写入workflow.yaml
    workflow = generate_workflow_chain(n)
    workflow_path = folder_path / 'workflow.yaml'
    with open(workflow_path, 'w', encoding='utf-8') as f:
        yaml.dump(workflow, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {workflow_path}")


def create_config_folder_parallel(n, base_path):
    """为给定的n创建Wn文件夹和配置文件（并行分支结构）"""
    folder_name = f'w{n}'
    folder_path = base_path / folder_name
    
    # 创建文件夹
    folder_path.mkdir(exist_ok=True)
    print(f"创建文件夹: {folder_path}")
    
    # 生成并写入function_info.yaml
    function_info = generate_function_info_parallel(n)
    function_info_path = folder_path / 'function_info.yaml'
    with open(function_info_path, 'w', encoding='utf-8') as f:
        yaml.dump(function_info, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {function_info_path}")
    
    # 生成并写入workflow.yaml
    workflow = generate_workflow_parallel(n)
    workflow_path = folder_path / 'workflow.yaml'
    with open(workflow_path, 'w', encoding='utf-8') as f:
        yaml.dump(workflow, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {workflow_path}")


def main():
    """主函数"""
    # 获取当前脚本所在目录
    script_dir = Path(__file__).parent
    base_path = script_dir
    
    print("开始生成微基准测试配置文件...")
    print(f"基础路径: {base_path}")
    
    # 为n=[2,4,8,16]生成配置
    n_values = [2, 4, 8, 16]
    
    # 生成链式结构的Cn文件夹
    print("\n=== 生成链式结构配置 (Cn) ===")
    for n in n_values:
        print(f"\n正在生成 C{n} 配置...")
        create_config_folder_chain(n, base_path)
    
    # 生成并行分支结构的Wn文件夹
    print("\n=== 生成并行分支结构配置 (Wn) ===")
    for n in n_values:
        print(f"\n正在生成 W{n} 配置...")
        create_config_folder_parallel(n, base_path)
    
    print("\n所有配置文件生成完成！")
    
    # 显示生成的目录结构
    print("\n生成的目录结构:")
    for n in n_values:
        # 链式结构
        chain_folder_path = base_path / f'C{n}'
        if chain_folder_path.exists():
            print(f"📁 C{n}/ (链式)")
            for file in sorted(chain_folder_path.iterdir()):
                if file.is_file():
                    print(f"  📄 {file.name}")
        
        # 并行分支结构
        parallel_folder_path = base_path / f'W{n}'
        if parallel_folder_path.exists():
            print(f"📁 W{n}/ (并行分支)")
            for file in sorted(parallel_folder_path.iterdir()):
                if file.is_file():
                    print(f"  📄 {file.name}")


if __name__ == "__main__":
    main()
