def main():
    func_input = store.fetch_input()
    num0 = func_input["chained_num_0"]
    chained_num_1 = num0 + 1
    store.ret({"chained_num_1":chained_num_1})