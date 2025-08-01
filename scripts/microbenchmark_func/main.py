import random
import string
import json

def generate_random_text(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def read_or_write_key(key, mode, payload_size):
    if mode == 'R':
        store.get(key)
    else:
        store.put(key, generate_random_text(payload_size))

def main():
    func_input = store.fetch_input()
    keys_info =  json.loads(func_input["keys"])
    payload_size = func_input["payload_size"]
    for key_info in keys_info:
        for key, mode in key_info.items():
            read_or_write_key(key, mode, payload_size)