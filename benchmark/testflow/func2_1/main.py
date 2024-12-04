def main():
    func_input = store.fetch_input()
    num0 = func_input["chained_num_1"]
    chained_num_2 = num0 + 2
    store.ret({"chained_num_2_1":chained_num_2})