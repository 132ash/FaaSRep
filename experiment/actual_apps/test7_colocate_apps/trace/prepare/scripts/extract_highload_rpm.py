import pandas as pd
import numpy as np
import os

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp")

import matplotlib.pyplot as plt
from pathlib import Path


script_dir = Path(__file__).parent.resolve()
PREPARE_DIR = script_dir.parent
SOURCE_CSV = PREPARE_DIR / "raw" / "2019trace" / "function_csvs" / "rank_0018_avgRPM_783.52_8f0c0b77db72a94503f488c825b918c454679f504c84da8e72.csv"
OUTPUT_CSV = PREPARE_DIR / "rpm" / "highload.csv"
OUTPUT_PNG = PREPARE_DIR / "rpm" / "highload.png"

def process_trace():
    # 1. Load data
    df = pd.read_csv(SOURCE_CSV)
    
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
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    new_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved to {OUTPUT_CSV}")
    
    # 7. Plot
    plt.figure(figsize=(10, 6))
    plt.plot(new_df['Minute'], new_df['RPM'], marker='o')
    plt.title('Processed RPM Trace (1 Hour, 2 Cycles)')
    plt.xlabel('Minute')
    plt.ylabel('RPM')
    plt.grid(True)
    plt.savefig(OUTPUT_PNG)
    print(f"Saved plot to {OUTPUT_PNG}")

if __name__ == "__main__":
    process_trace()
