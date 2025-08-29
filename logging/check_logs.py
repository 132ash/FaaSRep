import re

def find_incomplete_requests(log_file_path):
    """
    分析网关日志文件，找出那些已开始但未正常结束的请求。

    - 开始标志: 'processing request <tx_id>'
    - 结束标志 (满足其一即可):
        1. '[FINISH] transaction <tx_id> finished'
        2. '[ABORT] transaction <tx_id> aborted, active_abort: True'

    Args:
        log_file_path (str): 网关日志文件的路径。

    Returns:
        list: 包含所有未完成请求的事务 ID 的列表。
    """
    started_requests = set()
    completed_requests = set()

    # 正则表达式用于从日志行中提取事务 ID
    processing_regex = re.compile(r"processing request ([\w-]+)")
    finish_regex = re.compile(r"\[FINISH\] transaction ([\w-]+) finished")
    abort_regex = re.compile(r"\[ABORT\] transaction ([\w-]+) aborted, active_abort: True")

    try:
        with open(log_file_path, 'r') as f:
            for line in f:
                match = processing_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    started_requests.add(tx_id)
                    continue

                match = finish_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    completed_requests.add(tx_id)
                    continue

                match = abort_regex.search(line)
                if match:
                    tx_id = match.group(1)
                    completed_requests.add(tx_id)
                    continue

    except FileNotFoundError:
        print(f"错误: 日志文件未找到于 '{log_file_path}'")
        return []

    # 未完成的请求 = 已开始的请求 - 已完成的请求
    incomplete_ids = list(started_requests - completed_requests)
    return incomplete_ids

if __name__ == '__main__':
    # 根据用户提供的上下文，将默认日志文件名更改为 'gateway copy.log'
    log_file = './gateway.log'
    incomplete = find_incomplete_requests(log_file)

    if incomplete:
        print("发现以下未完成的请求：")
        for tx_id in incomplete:
            print(f"- {tx_id}")
    else:
        print("没有发现未完成的请求。")