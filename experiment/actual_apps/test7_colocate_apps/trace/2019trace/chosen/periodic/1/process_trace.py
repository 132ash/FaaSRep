import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def process_trace():
    file_path = '/home/shao/FaaSnap/experiment/actual_apps/test7_colocate_apps/trace/2019trace/chosen/periodic/1/rank_0018_avgRPM_783.52_8f0c0b77db72a94503f488c825b918c454679f504c84da8e72.csv'
    
    # 1. Load data
    df = pd.read_csv(file_path)
    
    # 2. Identify period (assuming daily pattern first, but let's check)
    # We'll look at a window around 5000
    # Let's assume 1440 minutes (1 day) is the period.
    period = 1440
    
    # Find the start of the "period around 5000".
    # 5000 is in the 4th day (starts at 4320).
    # Let's take from 4320 to 4320 + 2 * 1440 = 7200.
    start_idx = 4320
    end_idx = start_idx + 2 * period
    
    subset = df.iloc[start_idx:end_idx].copy()
    
    if len(subset) < 2 * period:
        print("Not enough data for 2 periods.")
        return

    print(f"Selected range: {start_idx} to {end_idx} (Length: {len(subset)})")
    
    # 3. Downsample to 60 minutes (1 hour)
    # We have 2880 points, we want 60 points.
    # Factor = 2880 / 60 = 48.
    # We will take the mean of every 48 points.
    
    resampled_rpm = []
    original_rpm = subset['RPM'].values
    
    chunk_size = len(subset) // 60
    for i in range(60):
        chunk = original_rpm[i*chunk_size : (i+1)*chunk_size]
        resampled_rpm.append(np.mean(chunk))
        
    # 4. Scale values by 3
    scaled_rpm = [x * 3 for x in resampled_rpm]
    
    # 5. Create new DataFrame
    new_df = pd.DataFrame({
        'Minute': range(1, 61),
        'RPM': scaled_rpm
    })
    
    # Round RPM to integer
    new_df['RPM'] = new_df['RPM'].astype(int)
    
    print(new_df.head())
    print(f"Min RPM: {new_df['RPM'].min()}, Max RPM: {new_df['RPM'].max()}")
    
    # 6. Save to CSV
    output_csv = 'processed_trace_1h.csv'
    new_df.to_csv(output_csv, index=False)
    print(f"Saved to {output_csv}")
    
    # 7. Plot
    plt.figure(figsize=(10, 6))
    plt.plot(new_df['Minute'], new_df['RPM'], marker='o')
    plt.title('Processed RPM Trace (1 Hour, 2 Cycles)')
    plt.xlabel('Minute')
    plt.ylabel('RPM')
    plt.grid(True)
    plt.savefig('processed_trace_1h.png')
    print("Saved plot to processed_trace_1h.png")

if __name__ == "__main__":
    process_trace()
