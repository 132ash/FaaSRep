import json
import os
import argparse

DEFAULT_WARMUP_SECONDS = 30


def get_segment_cutoff(data, warmup_seconds):
    segment_start_time = data.get('segment_start_local_time')
    if segment_start_time is not None:
        return segment_start_time + warmup_seconds

    ids = data.get('ids', {})
    start_times = [info['st'] for info in ids.values() if 'st' in info]
    if not start_times:
        return None
    return min(start_times) + warmup_seconds


def merge_results(result_dir, output_file, default_warmup_seconds=DEFAULT_WARMUP_SECONDS):
    print(f"Merging results from {result_dir}...")
    
    result_files = sorted([f for f in os.listdir(result_dir) if f.startswith('result_segment_') and f.endswith('.json')])
    
    merged_ids = {}
    
    for fname in result_files:
        fpath = os.path.join(result_dir, fname)
        print(f"Processing {fname}...")
        with open(fpath, 'r') as f:
            data = json.load(f)
            
        segment_ids = data['ids']
        warmup_seconds = data.get('warmup_seconds', default_warmup_seconds)
        cutoff = get_segment_cutoff(data, warmup_seconds)
        count = 0
        skipped = 0
        warmup_skipped = 0
        for req_id, info in segment_ids.items():
            if cutoff is not None and info.get('st', 0) < cutoff:
                warmup_skipped += 1
                continue
            if req_id not in merged_ids:
                merged_ids[req_id] = info
                count += 1
            else:
                skipped += 1
        print(f"  Added {count} requests, skipped {warmup_skipped} warmup requests, skipped {skipped} duplicates.")
        
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
        'warmup_cut_per_segment_seconds': default_warmup_seconds,
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
    parser.add_argument('--default-warmup-seconds', type=float, default=DEFAULT_WARMUP_SECONDS, help='Warmup seconds to cut if a segment result does not record warmup_seconds')
    args = parser.parse_args()
    
    merge_results(args.result_dir, args.output, args.default_warmup_seconds)
