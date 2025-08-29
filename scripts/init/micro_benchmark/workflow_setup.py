import sys
import yaml
from pathlib import Path

script_dir = Path(__file__).parent
# 添加正确的路径到 sys.path
sys.path.append(str(script_dir.parent.parent.parent))  # 添加项目根目录
sys.path.append(str(script_dir.parent.parent.parent / 'config'))  # 添加 config 目录

# 现在正确导入 repository
from scripts.init.repository import Repository

repo = Repository()

# ... 其余代码保持不变 ...

def generate_function_info_chain(n):
    function_info = {
        'workflow': f"c{n}",
        'functions': [],
        'max_containers': 64
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
        'max_containers': 64
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


def create_config_folder_chain(n, base_path):
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
    
    # 为n=[2,4,8,16]生成配置
    n_values = [2, 4, 6, 8]
    
    # 生成链式结构的Cn文件夹
    print("\n=== 生成链式结构配置 (cn) ===")
    for n in n_values:
        print(f"\n正在生成 c{n} 配置...")
        create_config_folder_chain(n, base_path)
    
    # 生成并行分支结构的Wn文件夹
    print("\n=== 生成并行分支结构配置 (wn) ===")
    for n in n_values:
        print(f"\n正在生成 w{n} 配置...")
        create_config_folder_parallel(n, base_path)
    
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