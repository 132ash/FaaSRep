import json
import sys
from pathlib import Path
import math

# Setup paths
script_dir = Path(__file__).parent.resolve()
def get_root_dir(script_dir: Path) -> Path:
    project_root = script_dir
    while project_root != project_root.parent:
        if (project_root / "config").is_dir() and (project_root / "experiment").is_dir():
            break
        project_root = project_root.parent
    return project_root

ROOT_DIR = get_root_dir(script_dir)
sys.path.append(str(ROOT_DIR))
PREPARE_DIR = script_dir.parent

from experiment.common import generate_param
workflow = 'travel_reservation'

def split_trace():
    # Configuration
    trace_file = PREPARE_DIR / 'raw' / 'trace_tidy.json'
    trace_id = 1
    start_idx = 105674
    exp_duration = 3600 # 1 hour
    
    core_segment_duration = 2 * 60 # 2 minutes used for measurement
    prefix_duration = 30 # 30 seconds warmup before each measured segment
    
    output_dir = PREPARE_DIR / 'segments' / 'lowload'
    output_dir.mkdir(parents=True, exist_ok=True)
    for old_segment in output_dir.glob('segment_*.json'):
        old_segment.unlink()
        
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
        
    # Split into measured 2-minute segments with a 30-second warmup prefix.
    num_segments = math.ceil(exp_duration / core_segment_duration)
    
    for i in range(num_segments):
        core_start_time = i * core_segment_duration
        core_end_time = min((i + 1) * core_segment_duration, exp_duration)
        
        actual_start_time = max(0, core_start_time - prefix_duration)
        actual_end_time = core_end_time
        warmup_seconds = core_start_time - actual_start_time
        
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
            'warmup_seconds': warmup_seconds,
            'requests': segment_requests,
            'workflow': workflow,
            'trace_id': trace_id,
            'base_start_timestamp': start_timestamp
        }
        
        outfile = output_dir / f'segment_{i}.json'
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(segment_data, f, indent=2)
            
        print(f"Created segment {i}: {len(segment_requests)} requests. Core: {core_start_time/60:.1f}-{core_end_time/60:.1f} min. Actual: {actual_start_time/60:.1f}-{actual_end_time/60:.1f} min")

if __name__ == "__main__":
    split_trace()
