import json
import os
import argparse
from pathlib import Path

def merge_results(result_dir, output_file):
    print(f"Merging results from {result_dir}...")
    
    result_files = sorted([f for f in os.listdir(result_dir) if f.startswith('result_segment_') and f.endswith('.json')])
    
    merged_ids = {}
    
    for fname in result_files:
        fpath = os.path.join(result_dir, fname)
        print(f"Processing {fname}...")
        with open(fpath, 'r') as f:
            data = json.load(f)
            
        segment_ids = data['ids']
        count = 0
        skipped = 0
        for req_id, info in segment_ids.items():
            if req_id not in merged_ids:
                merged_ids[req_id] = info
                count += 1
            else:
                skipped += 1
        print(f"  Added {count} requests, skipped {skipped} duplicates.")
        
    # Reconstruct lists
    # Sort by firing timestamp (st)
    sorted_reqs = sorted(merged_ids.values(), key=lambda x: x['st'])
    
    latencies = [r['e2e_latency'] for r in sorted_reqs]
    firing_timestamps = [r['st'] for r in sorted_reqs]
    
    # We might want to preserve other metadata from the first file
    workflow_name = "unknown"
    if result_files:
        with open(os.path.join(result_dir, result_files[0]), 'r') as f:
            first_data = json.load(f)
            workflow_name = first_data.get('workflow_name', 'unknown')

    merged_data = {
        'workflow_name': workflow_name,
        'latencies': latencies,
        'firing_timestamps': firing_timestamps,
        'ids': merged_ids
    }
    
    print(f"Saving merged results to {output_file}")
    print(f"Total unique requests: {len(merged_ids)}")
    if latencies:
        print(f"Average latency: {sum(latencies)/len(latencies):.3f}")
        
    with open(output_file, 'w') as f:
        json.dump(merged_data, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-dir', type=str, required=True, help='Directory containing segment result files')
    parser.add_argument('--output', type=str, required=True, help='Path to output merged JSON file')
    args = parser.parse_args()
    
    merge_results(args.result_dir, args.output)
