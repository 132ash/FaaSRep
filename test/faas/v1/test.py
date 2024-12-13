from datetime import datetime

def get_timestamp():
    # 获取当前时间，并格式化为字符串，精确到微秒
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    return timestamp

print(get_timestamp())