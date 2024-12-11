def main():
    func_input = store.fetch_input()
    num2_1 = func_input["chained_num_2_1"]
    num2_2 = func_input["chained_num_2_2"]
    test_value = int(store.get("test_value"))
    chained_num_final = num2_1 + num2_2 + test_value
    store.ret({"chained_num_final":chained_num_final})