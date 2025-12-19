import json
import sys
import os
from pathlib import Path
import math

# Setup paths
script_dir = Path(__file__).parent.resolve()
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "README.md").exists():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))

from experiment.common import generate_param

def split_trace():
    # Configuration
    trace_file = script_dir / 'trace_tidy.json'
    trace_id = 1
    start_idx = 105674
    exp_duration = 3600 # 1 hour
    workflow = 'travel_reservation'
    
    segment_duration = 5 * 60 # 5 minutes
    overlap_duration = 1 * 60 # 1 minute
    
    output_dir = script_dir / 'segments'
    if not output_dir.exists():
        os.makedirs(output_dir)
        
    print(f"Loading trace from {trace_file}...")
    with open(trace_file, 'r') as f:
        raw_trace = json.load(f)
        
    incoming_timestamps = raw_trace['per_function_invocations'][trace_id]['incoming_timestamps'][start_idx:]
    
    # Filter timestamps within experiment duration
    start_timestamp = incoming_timestamps[0] - 1
    last_timestamp = incoming_timestamps[0] + exp_duration
    
    valid_timestamps = []
    for ts in incoming_timestamps:
        if ts > last_timestamp:
            break
        valid_timestamps.append(ts)
    
    incoming_timestamps = valid_timestamps
    total_requests = len(incoming_timestamps)
    print(f"Total requests in 1 hour: {total_requests}")
    
    # Generate parameters for all requests
    print("Generating parameters...")
    # generate_workflow_inputs_for_clients returns a list of lists (clients -> rounds)
    # Here we treat it as 1 client with N rounds
    all_parameters = generate_param.generate_workflow_inputs_for_clients(workflow, 1, total_requests)[0]
    
    # Create full request objects
    all_requests = []
    for i in range(total_requests):
        req = {
            'global_req_id': i,
            'timestamp': incoming_timestamps[i],
            'relative_time': incoming_timestamps[i] - start_timestamp, # Time since experiment start (approx)
            'params': all_parameters[i]
        }
        all_requests.append(req)
        
    # Split into segments
    # Core segments: 0-5, 5-10, 10-15...
    num_segments = math.ceil(exp_duration / segment_duration)
    
    for i in range(num_segments):
        core_start_time = i * segment_duration
        core_end_time = (i + 1) * segment_duration
        
        # Apply overlap
        if i == 0:
            actual_start_time = core_start_time
        else:
            actual_start_time = core_start_time - overlap_duration
            
        actual_end_time = core_end_time
        
        # Filter requests for this segment
        # We use relative_time for filtering. 
        # Note: relative_time in all_requests starts from ~1 (since start_timestamp = ts[0] - 1)
        # Actually, let's use the timestamp directly.
        # Experiment start time = start_timestamp
        
        seg_start_abs = start_timestamp + actual_start_time
        seg_end_abs = start_timestamp + actual_end_time
        
        segment_requests = []
        for req in all_requests:
            # We include requests that fall into the time window
            # Using > and <= to be consistent with typical time intervals, but here we just check bounds
            if req['timestamp'] > seg_start_abs and req['timestamp'] <= seg_end_abs:
                segment_requests.append(req)
        
        segment_data = {
            'segment_index': i,
            'core_interval': [core_start_time, core_end_time],
            'actual_interval': [actual_start_time, actual_end_time],
            'requests': segment_requests,
            'workflow': workflow,
            'trace_id': trace_id,
            'base_start_timestamp': start_timestamp
        }
        
        outfile = output_dir / f'segment_{i}.json'
        with open(outfile, 'w') as f:
            json.dump(segment_data, f, indent=2)
            
        print(f"Created segment {i}: {len(segment_requests)} requests. Interval: {actual_start_time/60:.1f}-{actual_end_time/60:.1f} min")

if __name__ == "__main__":
    split_trace()
