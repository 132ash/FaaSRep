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
    retry_abort_func = func_input.get("retry_abort_func") or "NONE"
    store.set_transaction_metadata("retry_abort_func", retry_abort_func)
    if store.is_repair and retry_abort_func == function_name:
        store.abort_tx(
            f"INJECTED_DYNAMIC_ACCESS_ABORT target={function_name}"
        )
    all_keys = json.loads(func_input["keys"])
    self_keys_info = all_keys.pop(function_name) 
    payload_size = func_input["payload_size"]
    for key, mode in self_keys_info.items():
        read_or_write_key(key, mode, payload_size)
    store.ret({
        "payload_size": payload_size,
        "keys": json.dumps(all_keys),
        "retry_abort_func": retry_abort_func,
    })
