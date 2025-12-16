import time
import random
import string

# Create a dictionary with 50,000 string records
def create_large_dict(size=50000):
    """Generate a dictionary with string keys and values"""
    data_dict = {}
    for i in range(size):
        key = f"key_{i:06d}"
        value = ''.join(random.choices(string.ascii_letters, k=50))
        data_dict[key] = value
    return data_dict

# Measure query overhead
def measure_query_overhead(data_dict, num_queries=10000):
    """Measure the time cost of dictionary lookups"""
    keys = list(data_dict.keys())
    query_keys = random.choices(keys, k=num_queries)
    
    start_time = time.time()
    for key in query_keys:
        _ = data_dict[key]
    end_time = time.time()
    
    total_time = end_time - start_time
    avg_time = (total_time / num_queries) * 1_000_000  # Convert to microseconds
    
    return total_time, avg_time

if __name__ == "__main__":
    print("Creating dictionary with 50,000 records...")
    large_dict = create_large_dict(50000)
    print(f"Dictionary size: {len(large_dict)} records\n")
    
    print("Measuring query overhead for 10,000 lookups...")
    total_time, avg_time = measure_query_overhead(large_dict, 10000)
    
    print(f"Total time: {total_time:.6f} seconds")
    print(f"Average time per query: {avg_time:.3f} microseconds")