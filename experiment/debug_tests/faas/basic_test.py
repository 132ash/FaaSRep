test_batch = {'id1':[1,2,3]}

finished_list = [test_batch[id] for id in test_batch]
print(finished_list)  # Output: [[1, 2, 3]]
test_batch.pop('id1', None)
print(finished_list)  # Output: {}