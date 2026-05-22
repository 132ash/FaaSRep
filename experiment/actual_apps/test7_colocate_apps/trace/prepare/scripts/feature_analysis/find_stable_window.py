import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def main():
    input_file = 'top5_func_2_full.csv'
    output_csv = 'top5_func_2_stable_60min.csv'
    output_png = 'top5_func_2_stable_60min.png'
    
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"File {input_file} not found.")
        return

    # Identify the data column (not 'Minute')
    data_col = [c for c in df.columns if c != 'Minute'][0]
    series = df[data_col]
    minutes = df['Minute']
    
    window_size = 60
    min_rpm = 3500
    max_rpm = 4500
    
    valid_windows = []
    best_effort_windows = []
    
    print(f"Searching for 60-min windows with RPM between {min_rpm} and {max_rpm}...")
    
    for i in range(len(df) - window_size + 1):
        window = series.iloc[i : i + window_size]
        
        w_min = window.min()
        w_max = window.max()
        w_std = window.std()
        w_mean = window.mean()
        
        # Calculate violation
        violation = 0
        for val in window:
            if val < min_rpm:
                violation += (min_rpm - val)
            elif val > max_rpm:
                violation += (val - max_rpm)
        
        window_info = {
            'start_index': i,
            'std': w_std,
            'mean': w_mean,
            'violation': violation
        }

        if violation == 0:
            valid_windows.append(window_info)
        
        best_effort_windows.append(window_info)
            
    best_window_start = -1
    selected_info = None

    if valid_windows:
        print(f"Found {len(valid_windows)} strict windows.")
        # Sort by std ascending (most stable)
        valid_windows.sort(key=lambda x: x['std'])
        selected_info = valid_windows[0]
        best_window_start = selected_info['start_index']
        print("Selected best strict window.")
    else:
        print("No strict window found.")
        print("Searching for best effort window (minimizing violations, then minimizing std)...")
        
        # Sort by violation (asc), then std (asc)
        best_effort_windows.sort(key=lambda x: (x['violation'], x['std']))
        selected_info = best_effort_windows[0]
        best_window_start = selected_info['start_index']
        print("Selected best effort window.")

    print(f"Start Index: {best_window_start} (Minute {minutes.iloc[best_window_start]})")
    print(f"Mean RPM: {selected_info['mean']:.2f}")
    print(f"Std Dev: {selected_info['std']:.2f}")
    print(f"Total Violation: {selected_info['violation']:.2f}")

    # Extract best window
    result_df = df.iloc[best_window_start : best_window_start + window_size].copy()
    
    # Reset Minute to relative 1-60
    result_df['Minute'] = range(1, window_size + 1)
    
    # Save
    result_df.to_csv(output_csv, index=False)
    print(f"Saved to {output_csv}")
    
    # Prepare data for plotting (Seconds vs RPS)
    seconds_axis = []
    rps_values = []
    
    for _, row in result_df.iterrows():
        rpm = row[data_col]
        rps = rpm / 60.0
        # Each minute has 60 seconds
        for _ in range(60):
            seconds_axis.append(len(seconds_axis) + 1)
            rps_values.append(rps)
            
    # Plot
    plt.figure(figsize=(12, 6))
    plt.plot(seconds_axis, rps_values, label='RPS', linewidth=1.5)
    
    # Draw limit lines (converted to RPS)
    plt.axhline(y=min_rpm/60, color='r', linestyle='--', label=f'Min {min_rpm/60:.1f} RPS')
    plt.axhline(y=max_rpm/60, color='g', linestyle='--', label=f'Max {max_rpm/60:.1f} RPS')
    
    plt.xlabel('Time (Seconds)')
    plt.ylabel('Requests Per Second (RPS)')
    plt.title(f'Selected 60 Minutes Window (Stable RPM)\nOriginal Start Minute: {df["Minute"].iloc[best_window_start]}')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_png)
    print(f"Plot saved to {output_png}")

if __name__ == "__main__":
    main()
