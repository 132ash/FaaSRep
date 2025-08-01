#!/usr/bin/env python3
"""
测试节点分配逻辑
"""

def generate_node_assign_chain_test(n, all_workers):
    """生成链式工作流的节点分配（测试版本）"""
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
        worker_functions = []
        for _ in range(current_worker_functions):
            if current_function <= n:
                node_assign[f'f{current_function}'] = all_workers[worker_idx] + ":7500"
                worker_functions.append(f'f{current_function}')
                current_function += 1
        
        if worker_functions:
            print(f"节点 {all_workers[worker_idx]} 分配函数: {worker_functions}")
    
    return node_assign

def generate_node_assign_parallel_test(n, all_workers):
    """生成并行工作流的节点分配（测试版本）"""
    node_assign = {}
    num_workers = len(all_workers)
    
    # f1 分配到第一个节点
    node_assign['f1'] = all_workers[0] + ":7500"
    print(f"节点 {all_workers[0]} 分配函数: ['f1']")
    
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
            worker_functions = []
            for _ in range(current_worker_functions):
                if current_f2_idx < n:
                    node_assign[f'f2_{current_f2_idx}'] = all_workers[worker_idx] + ":7500"
                    worker_functions.append(f'f2_{current_f2_idx}')
                    current_f2_idx += 1
            
            if worker_functions:
                print(f"节点 {all_workers[worker_idx]} 分配并行函数: {worker_functions}")
    
    # f3 分配到最后一个节点
    node_assign['f3'] = all_workers[-1] + ":7500"
    print(f"节点 {all_workers[-1]} 分配函数: ['f3']")

    return node_assign

if __name__ == "__main__":
    # 模拟的工作节点
    test_workers = ['10.2.27.24', '10.2.30.52']
    
    print("=== 测试链式工作流节点分配 ===")
    for n in [2, 4, 8, 16]:
        print(f"\n链式工作流 c{n} (n={n}):")
        result = generate_node_assign_chain_test(n, test_workers)
        print(f"分配结果: {result}")
        
    print("\n=== 测试并行工作流节点分配 ===")
    for n in [2, 4, 8, 16]:
        print(f"\n并行工作流 w{n} (n={n}):")
        result = generate_node_assign_parallel_test(n, test_workers)
        print(f"分配结果: {result}")
