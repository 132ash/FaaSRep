import json
import sys
from pathlib import Path
import math
import pandas as pd

trace_name = 'highload'
workflow = 'social_network'

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
PREPARE_DIR = script_dir.parent


sys.path.append(str(ROOT_DIR))

from experiment.common import generate_param


def split_trace_2019():
    # Configuration
    csv_file = PREPARE_DIR / 'rpm' /  (trace_name + '.csv')
    exp_duration = 3600 # 1 hour
    
    core_segment_duration = 2.5 * 60 # 2 minutes used for measurement
    prefix_duration = 10 # 30 seconds warmup before each measured segment
    
    output_dir = PREPARE_DIR / 'segments' / trace_name
    output_dir.mkdir(parents=True, exist_ok=True)
    for old_segment in output_dir.glob('segment_*.json'):
        old_segment.unlink()
        
    print(f"Loading trace from {csv_file}...")
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: {csv_file} not found.")
        return
    
    # Generate timestamps
    print("Generating timestamps...")
    all_timestamps = []
    
    # Base timestamp (synthetic) - using an arbitrary start time
    start_timestamp = 1600000000.0 
    
    for _, row in df.iterrows():
        minute = int(row['Minute'])
        rpm = row['RPM']
        
        if rpm <= 0:
            continue
            
        # Calculate interval between requests to achieve this RPM
        # We distribute requests evenly across the 60 seconds of this minute
        interval = 60.0 / rpm
        
        # Start time of this minute (relative to 0)
        # Minute 1 starts at 0s, Minute 2 at 60s, etc.
        minute_start_rel = (minute - 1) * 60.0
        
        # Generate requests
        # We generate exactly 'rpm' requests for this minute
        count = int(rpm)
        for i in range(count):
            # Time relative to the start of the minute
            # We center them or start from 0? 
            # "Periodically" usually implies start at 0, interval, 2*interval...
            # But we should be careful not to exceed 60s if possible, though with count=RPM and interval=60/RPM, the last one is at 60 - interval.
            ts_rel_in_minute = i * interval
            
            ts_rel_total = minute_start_rel + ts_rel_in_minute
            
            # Ensure we don't go beyond the experiment duration (though Minute 60 goes up to 3600)
            if ts_rel_total >= exp_duration:
                continue
                
            all_timestamps.append(start_timestamp + ts_rel_total)
                
    total_requests = len(all_timestamps)
    print(f"Total requests generated: {total_requests}")
    
    # Generate parameters for all requests
    print("Generating parameters...")
    all_parameters = generate_param.generate_workflow_inputs_for_clients(workflow, 1, total_requests)[0]
    
    # Create full request objects
    all_requests = []
    for i in range(total_requests):
        ts = all_timestamps[i]
        req = {
            'global_req_id': i,
            'timestamp': ts,
            'relative_time': ts - start_timestamp, # Time since experiment start
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
        seg_start_abs = start_timestamp + actual_start_time
        seg_end_abs = start_timestamp + actual_end_time
        
        segment_requests = []
        for req in all_requests:
            # We include requests that fall into the time window
            if req['timestamp'] > seg_start_abs and req['timestamp'] <= seg_end_abs:
                segment_requests.append(req)
        
        segment_data = {
            'segment_index': i,
            'core_interval': [core_start_time, core_end_time],
            'actual_interval': [actual_start_time, actual_end_time],
            'warmup_seconds': warmup_seconds,
            'requests': segment_requests,
            'workflow': workflow,
            'trace_id': '2019_synthetic',
            'base_start_timestamp': start_timestamp
        }
        
        outfile = output_dir / f'segment_{i}.json'
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(segment_data, f, indent=2)
            
        print(f"Created segment {i}: {len(segment_requests)} requests. Core: {core_start_time/60:.1f}-{core_end_time/60:.1f} min. Actual: {actual_start_time/60:.1f}-{actual_end_time/60:.1f} min")

if __name__ == "__main__":
    split_trace_2019()
