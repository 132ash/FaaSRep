import sys
import yaml
from pathlib import Path

script_dir = Path(__file__).parent
sys.path.append(str(script_dir.parent))
sys.path.append(str(script_dir.parent.parent / 'config'))
import config
from repository import Repository

repo = Repository()

all_workers = repo.get_all_addrs()

def generate_function_info_chain(n):
    function_info = {
        'workflow': f"c{n}",
        'functions': [],
        'max_containers': 30
    }
    
    for i in range(1, n + 1):
        function_info['functions'].append({
            'image': 'micro_func',
            'name': f'f{i}'
        })
    
    return function_info


def generate_function_info_parallel(n):
    function_info = {
        'workflow': f'w{n}',
        'functions': [],
        'max_containers': 30
    }
    
    function_info['functions'].append({
        'image': 'micro_func',
        'name': 'f1'
    })
    
    for i in range(1, n):
        function_info['functions'].append({
            'image': 'micro_func',
            'name': f'f2_{i}'
        })
    
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
    
    for i in range(1, n + 1):
        function_def = {
            'name': f'f{i}',
            'source': f'f{i}',
            'input': {
                'keys': {
                    'from': 'GLOBAL' if i == 1 else f'f{i-1}',
                    'type': 'str'
                },
                'payload_size': {
                    'from': 'GLOBAL' if i == 1 else f'f{i-1}',
                    'type': 'int'
                }
            },
            'output': {
                'keys': {
                    'type': 'str'
                },
                'payload_size': {
                    'type': 'int'
                }
            }
        }
        
        if i < n: 
            function_def['next'] = {
                'type': 'pass',
                'nodes': [f'f{i + 1}']
            }
        else:  
            function_def['next'] = {
                'type': 'FINISH'
            }
        
        workflow['functions'].append(function_def)
    
    return workflow


def generate_workflow_parallel(n):
    workflow = {
        'functions': []
    }
    
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
        'output': {
            'keys': {
                'type': 'str'
            },
            'payload_size': {
                'type': 'int'
            }
        },
        'next': {
            'type': 'pass',
            'nodes': f1_nodes
        }
    }
    workflow['functions'].append(f1_def)
    
    for i in range(1, n):
        f2_def = {
            'name': f'f2_{i}',
            'source': f'f2_{i}',
            'input': {
                'keys': {
                    'from': 'f1',
                    'type': 'str'
                },
                'payload_size': {
                    'from': 'f1',
                    'type': 'int'
                }
            },
            'output': {
                'keys': {
                    'type': 'str'
                },
                'payload_size': {
                    'type': 'int'
                }
            },
            'next': {
                'type': 'pass',
                'nodes': ['f3']
            }
        }
        workflow['functions'].append(f2_def)
    f3_def = {
        'name': 'f3',
        'source': 'f3',
        'input': {
            'keys': {
                'from': 'f1',
                'type': 'str'
            },
            'payload_size': {
                'from': 'f1',
                'type': 'int'
            }
        },
        'output': {
            'keys': {
                'type': 'str'
            },
            'payload_size': {
                'type': 'int'
            }
        },
        'next': {
            'type': 'FINISH'
        }
    }
    workflow['functions'].append(f3_def)
    
    return workflow


def generate_node_assign_chain(n, all_workers):
    """生成链式工作流的节点分配"""
    node_assign = {}
    
    # 将函数分组，使相邻函数尽可能在同一节点
    num_workers = len(all_workers)
    functions_per_worker = n // num_workers  # 每个节点分配的基础函数数量
    extra_functions = n % num_workers  # 剩余函数数量
    
    current_function = 1
    for worker_idx in range(num_workers):
        # 计算当前节点应该分配的函数数量
        current_worker_functions = functions_per_worker
        if worker_idx < extra_functions:  # 前几个节点分配额外的函数
            current_worker_functions += 1
        
        # 为当前节点分配连续的函数
        for _ in range(current_worker_functions):
            if current_function <= n:
                node_assign[f'f{current_function}'] = all_workers[worker_idx] + f":{config.WORKERSP_PORT}"
                current_function += 1
    
    return node_assign


def generate_node_assign_parallel(n, all_workers):
    """生成并行工作流的节点分配"""
    node_assign = {}
    num_workers = len(all_workers)
    
    # f1 分配到第一个节点
    node_assign['f1'] = all_workers[0] + f":{config.WORKERSP_PORT}"
    
    # f2_1 到 f2_(n-1) 分组分配到节点
    f2_functions = n - 1  # f2_1 到 f2_(n-1) 的数量
    if f2_functions > 0:
        functions_per_worker = f2_functions // num_workers
        extra_functions = f2_functions % num_workers
        
        current_f2_idx = 1
        for worker_idx in range(num_workers):
            # 计算当前节点应该分配的f2函数数量
            current_worker_functions = functions_per_worker
            if worker_idx < extra_functions:
                current_worker_functions += 1
            
            # 为当前节点分配连续的f2函数
            for _ in range(current_worker_functions):
                if current_f2_idx < n:
                    node_assign[f'f2_{current_f2_idx}'] = all_workers[worker_idx] + f":{config.WORKERSP_PORT}"
                    current_f2_idx += 1
    
    # f3 分配到最后一个节点
    node_assign['f3'] = all_workers[-1] + f":{config.WORKERSP_PORT}"

    return node_assign


def create_config_folder_chain(n, base_path, all_workers):
    """为给定的n创建Cn文件夹和配置文件（链式结构）"""
    folder_name = f'c{n}'
    folder_path = base_path / folder_name
    
    folder_path.mkdir(exist_ok=True)
    print(f"创建文件夹: {folder_path}")
    
    function_info = generate_function_info_chain(n)
    function_info_path = folder_path / 'function_info.yaml'
    with open(function_info_path, 'w', encoding='utf-8') as f:
        yaml.dump(function_info, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {function_info_path}")
    
    workflow = generate_workflow_chain(n)
    workflow_path = folder_path / 'workflow.yaml'
    with open(workflow_path, 'w', encoding='utf-8') as f:
        yaml.dump(workflow, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {workflow_path}")
    
    # 生成并写入node_assign.yaml
    node_assign = generate_node_assign_chain(n, all_workers)
    node_assign_path = folder_path / 'node_assign.yaml'
    with open(node_assign_path, 'w', encoding='utf-8') as f:
        yaml.dump(node_assign, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {node_assign_path}")


def create_config_folder_parallel(n, base_path, all_workers):
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
    
    # 生成并写入node_assign.yaml
    node_assign = generate_node_assign_parallel(n, all_workers)
    node_assign_path = folder_path / 'node_assign.yaml'
    with open(node_assign_path, 'w', encoding='utf-8') as f:
        yaml.dump(node_assign, f, default_flow_style=False, allow_unicode=True, indent=2)
    print(f"创建文件: {node_assign_path}")


def main():
    """主函数"""
    script_dir = Path(__file__).parent
    project_root = script_dir
    while project_root != project_root.parent:
        # 检查是否是FaaSnap项目根目录（包含特征文件如README.md, build.sh等）
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    
    # 设置目标路径为项目根目录下的benchmark/micro_benchmark
    base_path = project_root / "benchmark" / "micro_benchmark"
    
    # 确保目标目录存在
    base_path.mkdir(parents=True, exist_ok=True)
    
    print(f"项目根目录: {project_root}")
    print(f"基础路径: {base_path}")
    print(f"可用工作节点: {all_workers}")
    
    # 检查是否获取到工作节点
    if not all_workers:
        print("警告: 无法获取工作节点信息，请检查数据库连接")
        return
    
    # 为n=[2,4,8,16]生成配置
    n_values = [2, 4, 8, 16]
    
    # 生成链式结构的Cn文件夹
    print("\n=== 生成链式结构配置 (cn) ===")
    for n in n_values:
        print(f"\n正在生成 c{n} 配置...")
        create_config_folder_chain(n, base_path, all_workers)
    
    # 生成并行分支结构的Wn文件夹
    print("\n=== 生成并行分支结构配置 (wn) ===")
    for n in n_values:
        print(f"\n正在生成 w{n} 配置...")
        create_config_folder_parallel(n, base_path, all_workers)
    
    print("\n所有配置文件生成完成！")
    
    # 显示生成的目录结构
    print("\n生成的目录结构:")
    for n in n_values:
        # 链式结构
        chain_folder_path = base_path / f'c{n}'
        if chain_folder_path.exists():
            print(f"📁 c{n}/ (链式)")
            for file in sorted(chain_folder_path.iterdir()):
                if file.is_file():
                    print(f"  📄 {file.name}")
        
        # 并行分支结构
        parallel_folder_path = base_path / f'w{n}'
        if parallel_folder_path.exists():
            print(f"📁 w{n}/ (并行分支)")
            for file in sorted(parallel_folder_path.iterdir()):
                if file.is_file():
                    print(f"  📄 {file.name}")


if __name__ == "__main__":
    main()
