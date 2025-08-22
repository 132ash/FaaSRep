import re

def find_incomplete_requests(log_file_path):
    """
    分析网关日志文件，找出未完成的请求。

    一个请求由一个唯一的事务 ID 标识。
    - 开始标志：'processing request <tx_id>'
    - 完成标志：'transaction <tx_id> ... aborted: ... clearing states'

    Args:
        log_file_path (str): gateway.log 文件的路径。

    Returns:
        list: 包含所有未完成请求的事务 ID 的列表。
    """
    started_requests = set()
    completed_requests = set()

    # 正则表达式用于从日志行中提取事务 ID
    processing_regex = re.compile(r"processing request ([\w-]+)")
    completion_regex = re.compile(r"transaction ([\w-]+) .* aborted:.* clearing states")

    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                # 检查开始日志
                match = processing_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    started_requests.add(tx_id)
                    continue

                # 检查完成日志
                match = completion_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    completed_requests.add(tx_id)
                    continue

    except FileNotFoundError:
        print(f"错误: 日志文件未找到于 '{log_file_path}'")
        return []

    # 未完成的请求是那些已经开始但尚未完成的请求
    # 使用集合的差集运算可以高效地找出这些请求
    incomplete_requests = list(started_requests - completed_requests)

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