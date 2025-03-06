def main():
    func_input = store.fetch_input()
    num0 = func_input["chained_num_0"]
    test_value = int(store.get("test_value"))
    chained_num_1 = num0 + 1 + test_value
    store.ret({"chained_num_1":chained_num_1})