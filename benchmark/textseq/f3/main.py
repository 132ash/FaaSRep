import random

def main():
    func_input = store.fetch_input()
    combined_content = []
    combined_content.append(func_input["t5"])
    combined_content.append(store.get("t4"))
    combined_content.append(store.get("t6"))
    combined_content = ''.join(combined_content)
    shuffled_content = list(combined_content)
    random.shuffle(shuffled_content)
    shuffled_content = ''.join(shuffled_content)
    part_size = len(shuffled_content) // 3
    store.put('t6', shuffled_content[0 : part_size])
    store.ret({"t7":shuffled_content[part_size:2*part_size]})