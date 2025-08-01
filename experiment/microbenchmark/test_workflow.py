#!/usr/bin/env python3
"""
测试 workflow.yaml 生成逻辑
"""
import yaml

def generate_workflow_chain_test(n):
    """生成链式workflow.yaml内容（测试版本）"""
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

def generate_workflow_parallel_test(n):
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

if __name__ == "__main__":
    print("=== 测试链式工作流 workflow.yaml 生成 ===")
    for n in [2, 4]:
        print(f"\n链式工作流 c{n}:")
        workflow = generate_workflow_chain_test(n)
        print(yaml.dump(workflow, default_flow_style=False, allow_unicode=True, indent=2))
        
    print("\n=== 测试并行工作流 workflow.yaml 生成 ===")
    for n in [2, 4]:
        print(f"\n并行工作流 w{n}:")
        workflow = generate_workflow_parallel_test(n)
        print(yaml.dump(workflow, default_flow_style=False, allow_unicode=True, indent=2))
