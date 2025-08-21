import re
from collections import defaultdict

def find_incomplete_requests(log_file_path):
    """
    分析网关日志文件，找出未完成的请求。

    一个请求由一个唯一的事务 ID 标识。
    - 开始标志：'processing request <tx_id>'
    - 完成标志（必须全部出现）：
        1. 'transaction <tx_id> ... finished running'
        2. 'transaction <tx_id> ... aborted: ... clearing states'
        3. 'transaction <tx_id> ... cleaned, return results'

    Args:
        log_file_path (str): geteway.log 文件的路径。

    Returns:
        list: 包含所有未完成请求的事务 ID 的列表。
    """
    # 使用 defaultdict 来存储每个事务 ID 遇到的日志阶段
    requests_status = defaultdict(set)

    # 正则表达式用于从日志行中提取事务 ID
    processing_regex = re.compile(r"processing request ([\w-]+)")
    finished_regex = re.compile(r"transaction ([\w-]+) .* finished running")
    aborted_regex = re.compile(r"transaction ([\w-]+) .* aborted:.* clearing states")
    cleaned_regex = re.compile(r"transaction ([\w-]+) .* cleaned, return results")

    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                match = processing_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    requests_status[tx_id].add('processing')
                    continue

                match = finished_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    requests_status[tx_id].add('finished')
                    continue

                match = aborted_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    requests_status[tx_id].add('aborted')
                    continue
                
                match = cleaned_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    requests_status[tx_id].add('cleaned')
                    continue

    except FileNotFoundError:
        print(f"错误: 日志文件未找到于 '{log_file_path}'")
        return []

    incomplete_requests = []
    for tx_id, stages in requests_status.items():
        # 一个请求已开始但未完全结束
        is_started = 'processing' in stages
        is_completed = {'finished', 'aborted', 'cleaned'}.issubset(stages)
        
        if is_started and not is_completed:
            incomplete_requests.append(tx_id)

    return incomplete_requests

if __name__ == '__main__':
    log_file = './gateway.log'
    incomplete = find_incomplete_requests(log_file)

    if incomplete:
        print("发现以下未完成的请求：")
        for tx_id in incomplete:
            print(f"- {tx_id}")
    else:
        print("没有发现未完成的请求。")