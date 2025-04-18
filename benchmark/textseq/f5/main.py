import random

def main():
    func_input = store.fetch_input()
    combined_content = []
    combined_content.append(func_input["t13"])
    combined_content.append(store.get("t12"))
    combined_content.append(store.get("t14"))
    combined_content = ''.join(combined_content)
    shuffled_content = list(combined_content)
    random.shuffle(shuffled_content)
    shuffled_content = ''.join(shuffled_content)
    part_size = len(shuffled_content) // 3
    store.put('t15', shuffled_content[0 : part_size])
    store.ret({"t16":shuffled_content[part_size:2*part_size]})