from pathlib import Path
import component
import sys

def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

script_dir = Path(__file__).parent
ROOT_DIR = get_root_dir(script_dir)
CONFIG_DIR = ROOT_DIR / 'config'
sys.path.append(str(CONFIG_DIR))
import config

WORKERSP_PORT = config.WORKERSP_PORT

def assign_function_to_node(workflow: component.workflow, all_worker_node: list, sink_node_addr: str):
    """
    将工作流函数分配到工作节点
    
    Args:
        workflow: 工作流对象
        all_worker_node: 所有工作节点列表
        sink_node_addr: sink节点地址
    
    Returns:
        dict: {function_name: "node_ip:WORKERSP_PORT"} 的字典
    """
    function_names = list(workflow.nodes.keys())
    num_functions = len(function_names)
    num_workers = len(all_worker_node)
    end_function_name = workflow.end_function['name']

    if num_functions == 0:
        return {}
    
    # 如果函数数量少于等于工作节点数量，每个函数分配一个节点
    if num_functions <= num_workers:
        assignment = {}
        
        # 确保 end_function 分配到 sink_node_addr
        if end_function_name in function_names:
            assignment[end_function_name] = f"{sink_node_addr}:{WORKERSP_PORT}"
            remaining_functions = [f for f in function_names if f != end_function_name]
            remaining_workers = [w for w in all_worker_node if w != sink_node_addr]
        else:
            remaining_functions = function_names[:]
            remaining_workers = all_worker_node[:]
        
        # 分配剩余函数
        for i, func_name in enumerate(remaining_functions):
            worker_idx = i % len(remaining_workers) if remaining_workers else 0
            worker_node = remaining_workers[worker_idx] if remaining_workers else all_worker_node[0]
            assignment[func_name] = f"{worker_node}:{WORKERSP_PORT}"
            
        return assignment
    
    # 函数数量大于工作节点数量时，需要分组
    # 计算每组的平均大小
    base_group_size = num_functions // num_workers
    extra_functions = num_functions % num_workers
    
    # 构建函数的拓扑顺序（基于依赖关系）
    ordered_functions = topological_sort(workflow)
    
    # 将函数分组
    groups = []
    current_idx = 0
    
    for i in range(num_workers):
        # 确定当前组的大小
        group_size = base_group_size + (1 if i < extra_functions else 0)
        
        # 分配函数到当前组
        group = ordered_functions[current_idx:current_idx + group_size]
        groups.append(group)
        current_idx += group_size
    
    # 确保 end_function 在指定的 sink 组中
    end_function_group_idx = None
    for i, group in enumerate(groups):
        if end_function_name in group:
            end_function_group_idx = i
            break
    
    # 如果需要，调整分组以确保 end_function 在 sink_node_addr 上
    sink_node_idx = all_worker_node.index(sink_node_addr) if sink_node_addr in all_worker_node else 0
    
    if end_function_group_idx is not None and end_function_group_idx != sink_node_idx:
        # 交换分组
        groups[end_function_group_idx], groups[sink_node_idx] = groups[sink_node_idx], groups[end_function_group_idx]
    
    # 生成最终的分配字典
    assignment = {}
    for i, group in enumerate(groups):
        worker_node = all_worker_node[i]
        for func_name in group:
            assignment[func_name] = f"{worker_node}:{WORKERSP_PORT}"
    
    return assignment


def topological_sort(workflow: component.workflow):
    """
    对工作流函数进行拓扑排序，确保相邻函数尽量在一起
    
    Args:
        workflow: 工作流对象
    
    Returns:
        list: 拓扑排序后的函数名列表
    """
    from collections import defaultdict, deque
    
    # 构建邻接表和入度统计
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_functions = set(workflow.nodes.keys())
    
    # 初始化入度
    for func_name in all_functions:
        in_degree[func_name] = 0
    
    # 构建图
    for func_name, func in workflow.nodes.items():
        for next_node in func.next:
            if next_node in all_functions:
                graph[func_name].append(next_node)
                in_degree[next_node] += 1

    # 拓扑排序
    queue = deque()
    result = []
    
    # 将入度为0的节点加入队列（通常是 start_functions）
    for func_name in all_functions:
        if in_degree[func_name] == 0:
            queue.append(func_name)
    
    # 如果没有入度为0的节点，从 start_functions 开始
    if not queue and workflow.start_functions:
        for start_func in workflow.start_functions:
            if start_func in all_functions:
                queue.append(start_func)
    
    # 如果还是没有，就从所有函数中选择第一个
    if not queue and all_functions:
        queue.append(list(all_functions)[0])
    
    while queue:
        current = queue.popleft()
        result.append(current)
        
        # 处理当前节点的邻居
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # 如果还有未访问的节点（可能是孤立节点），添加到结果中
    remaining = all_functions - set(result)
    result.extend(list(remaining))
    
    return result