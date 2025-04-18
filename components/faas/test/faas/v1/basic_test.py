import os
import random
import string
import time

def generate_text_files():
    """生成3个1MB大小的文本文件"""
    os.makedirs("text_files", exist_ok=True)
    for i in range(3):
        with open(f"text_files/file_{i + 1}.txt", "w") as f:
            content = ''.join(random.choices(string.ascii_letters + string.digits, k=1024 * 1024))
            f.write(content)

def process_and_create_new_files():
    start_time = time.time()
    combined_content = []

    # 读取文件内容
    for i in range(3):
        with open(f"text_files/file_{i + 1}.txt", "r") as f:
            combined_content.append(f.read())

    # 合并并随机打散
    combined_content = ''.join(combined_content)
    shuffled_content = list(combined_content)
    random.shuffle(shuffled_content)
    shuffled_content = ''.join(shuffled_content)

    s1 = str(shuffled_content)
    print(type(s1))

    # 截取两段4KB大小的文本并写入新文件
    os.makedirs("output_files", exist_ok=True)
    part_size = len(shuffled_content) // 3
    for i in range(2):
        with open(f"output_files/output_file_{i + 1}.txt", "w") as f:
            f.write(shuffled_content[i * part_size:(i + 1) * part_size])

    end_time = time.time()
    print(f"Processing time: {(end_time - start_time) * 1000:.2f} milliseconds")

# 调用函数
generate_text_files()
process_and_create_new_files()