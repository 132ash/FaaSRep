from gevent import monkey
monkey.patch_all()
import gevent
import boto3
import gc
import json
import sys
import time
import os
import argparse
from tqdm import tqdm
import requests
from pathlib import Path

script_dir = Path(__file__).parent
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

import config.config as config
from gevent.pool import Pool

DB_NODE_IP = config.STORAGE_NODE_IP
# dynamodb  = boto3.resource('dynamodb', endpoint_url=f'http://{DB_NODE_IP}:4567', aws_secret_access_key='FAASNAPDYNAMODBKEY', aws_access_key_id='FAASNAPDYNAMODB', region_name='us-west-2')

# Global variables for results
ids = {}
latencies = []
firing_timestamps = []

def gc_loop():
    while True:
        gevent.sleep(300)
        gc.collect()

def post_request(workflow, global_req_id, parameters_input):
    try:
        st = time.time()
        rep = run_workflow(workflow, parameters_input)
        ed = time.time()
        
        # Use global_req_id as key
        req_key = str(global_req_id)
        
        if rep.get('failed', False):
            print(f"Request {req_key} failed for workflow {workflow}.")
            return
            
        ids[req_key] = {
            'time': ed - st, 
            'st': st, 
            'ed': ed, 
            'e2e_latency': rep['e2e_latency'], 
            'rounds': rep['rounds'],
            'global_req_id': global_req_id
        }
        latencies.append(rep['e2e_latency'])
        firing_timestamps.append(st)
    except Exception as e:
        print(f"Error in post_request for workflow {workflow}: {e}")

def run_workflow(workflow_name, parameters):
    url = f'http://{config.GATEWAY_ADDR}/run'
    inputs = {'workflow':workflow_name, 'parameters':json.dumps(parameters)}
    transaction_id = parameters.pop('transaction_id', None)
    if transaction_id:
        inputs['transaction_id'] = transaction_id
    try:
        rep = requests.post(url, json = inputs)
        rep.raise_for_status()
        return rep.json()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return {'e2e_latency': 0, 'rounds': 0, 'failed': True}

def save_results(filepath, workflow_name, segment_index):
    print(f"\nSaving results to {filepath}...")
    save_logs = {
        'workflow_name': workflow_name,
        'segment_index': segment_index,
        'latencies': latencies,
        'firing_timestamps': firing_timestamps,
        'ids': ids
    }
    
    # Atomic write
    temp_path = filepath + '.tmp'
    with open(temp_path, 'w') as f:
        json.dump(save_logs, f)
    os.rename(temp_path, filepath)

def run(segment_file, output_file):
    print(f"Loading segment from {segment_file}")
    with open(segment_file, 'r') as f:
        segment_data = json.load(f)
        
    requests_data = segment_data['requests']
    workflow = segment_data['workflow']
    segment_index = segment_data['segment_index']
    base_start_timestamp = segment_data['base_start_timestamp']
    
    print(f"Running segment {segment_index} with {len(requests_data)} requests.")
    
    # Determine start time for this run
    # We want to simulate the timing relative to the segment start.
    # The requests have absolute timestamps.
    # We should align the first request (or the segment start time) to now.
    
    # Option 1: Align based on absolute timestamps relative to base_start_timestamp
    # But we are running segments independently.
    # The segment has 'actual_interval' [start_offset, end_offset].
    # We can treat 'now' as base_start_timestamp + start_offset.
    
    actual_start_offset = segment_data['actual_interval'][0]
    
    # However, we just want to replay the trace with correct delays between requests.
    # And correct initial delay.
    
    # Let's say the segment starts at T=240s (relative to exp start).
    # The first request in this segment might be at T=241s.
    # We should start the experiment, and fire the first request at (241 - 240) = 1s later?
    # Or should we just start firing immediately if the timestamp is passed?
    
    # The user said "read relevant fragment and experiment".
    # Usually in trace replay, we want to preserve the inter-arrival times.
    # And if we are simulating a specific window, we should respect the relative timing within that window.
    
    # Let's define T_local_start = time.time()
    # This corresponds to the beginning of the segment (actual_start_offset).
    # So for a request at T_req (absolute), its relative time to segment start is:
    # T_rel_seg = T_req - (base_start_timestamp + actual_start_offset)
    # We should fire it at T_local_start + T_rel_seg.
    
    start_local_time = time.time()
    
    # Sort requests just in case
    requests_data.sort(key=lambda x: x['timestamp'])
    
    pool = Pool(10000)
    
    for i, req in enumerate(tqdm(requests_data)):
        req_ts = req['timestamp']
        # Calculate delay
        # Target time relative to experiment start: req_ts - base_start_timestamp
        # Target time relative to segment start: (req_ts - base_start_timestamp) - actual_start_offset
        
        target_delay = (req_ts - base_start_timestamp) - actual_start_offset
        
        # If target_delay is negative (request is before segment start?), it shouldn't happen if we filtered correctly.
        # But if it is, we fire immediately.
        
        current_elapsed = time.time() - start_local_time
        wait_time = max(0, target_delay - current_elapsed)
        
        if wait_time > 0:
            gevent.sleep(wait_time)
            
        global_req_id = req['global_req_id']
        params = req['params']
        
        pool.spawn(post_request, workflow, global_req_id, params)

    pool.join()
    
    # Wait a bit for trailing responses
    gevent.sleep(5)
    
    save_results(output_file, workflow, segment_index)
    
    print('total requests count:', len(latencies))
    if latencies:
        print('avg:', format(sum(latencies) / len(latencies), '.3f'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--segment', type=str, required=True, help='Path to segment JSON file')
    parser.add_argument('--output', type=str, required=True, help='Path to output JSON file')
    args = parser.parse_args()
    
    gevent.spawn(gc_loop)
    run(args.segment, args.output)
