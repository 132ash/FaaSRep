import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
import shutil

def main():
    # --- Configuration ---
    input_pattern = 'invocations_per_function_md.anon.d*.csv'
    csv_output_dir = 'function_csvs'
    plot_output_dir = 'function_plots'
    
    # Create output directories (clean them if they exist to avoid stale data)
    for d in [csv_output_dir, plot_output_dir]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)

    # 1. Find all csv files
    files = sorted(glob.glob(input_pattern))
    if not files:
        print("No csv files found.")
        return

    print(f"Found {len(files)} files.")

    # Columns representing minutes in the source files (1 to 1440)
    minute_cols = [str(i) for i in range(1, 1441)]
    
    # Dictionary to store time series for ALL functions
    # Key: (HashOwner, HashApp, HashFunction)
    # Value: List of invocations (length should eventually be 14 * 1440)
    all_function_series = {}

    print("Reading files and extracting time series...")
    
    # We need to process files in order (d01, d02, ...) to build the timeline correctly
    for f in files:
        print(f"Processing {f}...")
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
        
        # Filter Trigger == 'http'
        df_http = df[df['Trigger'] == 'http'].copy()
        
        # Set index for quick lookup
        df_http.set_index(['HashOwner', 'HashApp', 'HashFunction'], inplace=True)
        
        # Get the set of functions present in this file
        current_file_funcs = set(df_http.index)
        
        # Get the set of functions we have already seen
        known_funcs = set(all_function_series.keys())
        
        # Union of all functions
        all_funcs = current_file_funcs.union(known_funcs)
        
        for func_id in all_funcs:
            # If it's a new function we haven't seen before, pad previous days with 0
            if func_id not in all_function_series:
                # Find current expected length based on the first function in the dict (if exists)
                if all_function_series:
                    current_len = len(next(iter(all_function_series.values())))
                else:
                    current_len = 0 # First file, first function
                
                all_function_series[func_id] = [0] * current_len

            # Extract data for this file
            if func_id in df_http.index:
                row = df_http.loc[func_id]
                # Handle duplicate rows for same function (sum them up)
                if isinstance(row, pd.DataFrame):
                    vals = row[minute_cols].sum(axis=0).values.tolist()
                else:
                    vals = row[minute_cols].values.tolist()
                all_function_series[func_id].extend(vals)
            else:
                # Function exists in history but not in this file -> append 0s
                all_function_series[func_id].extend([0] * 1440)

    if not all_function_series:
        print("No http functions found.")
        return

    print(f"Total unique HTTP functions found: {len(all_function_series)}")

    # --- Analysis and Output ---
    
    # Calculate stats for sorting
    func_stats = []
    if all_function_series:
        total_minutes = len(next(iter(all_function_series.values()))) # Should be ~20160
    else:
        total_minutes = 1
    
    print("Calculating statistics...")
    for func_id, series in all_function_series.items():
        total_invocations = sum(series)
        avg_rpm = total_invocations / total_minutes
        func_stats.append({
            'func_id': func_id,
            'series': series,
            'avg_rpm': avg_rpm
        })
    
    # Sort by Avg RPM descending
    func_stats.sort(key=lambda x: x['avg_rpm'], reverse=True)
    
    print(f"Generating output files in '{csv_output_dir}' and '{plot_output_dir}'...")
    
    # Generate CSVs and Plots
    for rank, item in enumerate(func_stats):
        func_id = item['func_id']
        series = item['series']
        avg_rpm = item['avg_rpm']
        
        # Create a safe filename string
        # Format: rank_001_avgRPM_123.45_Owner_App_Func
        func_name_str = f"{func_id[0]}_{func_id[1]}_{func_id[2]}"
        # Truncate func name if too long to avoid filesystem errors
        if len(func_name_str) > 50:
            func_name_str = func_name_str[:50]
            
        base_filename = f"rank_{rank+1:04d}_avgRPM_{avg_rpm:.2f}_{func_name_str}"
        
        # 1. Save CSV
        csv_path = os.path.join(csv_output_dir, f"{base_filename}.csv")
        df_out = pd.DataFrame({
            'Minute': range(1, len(series) + 1),
            'RPM': series
        })
        df_out.to_csv(csv_path, index=False)
        
        # 2. Save Plot
        plot_path = os.path.join(plot_output_dir, f"{base_filename}.png")
        
        plt.figure(figsize=(12, 6))
        plt.plot(range(1, len(series) + 1), series, linewidth=0.5)
        plt.title(f"Rank: {rank+1} | Avg RPM: {avg_rpm:.2f}\n{func_id}")
        plt.xlabel("Minute (14 Days)")
        plt.ylabel("Invocations per Minute")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(plot_path, dpi=100)
        plt.close() # Close memory to prevent leak
        
        if (rank + 1) % 100 == 0:
            print(f"Processed {rank + 1} functions...")

    print("Done.")

if __name__ == "__main__":
    main()
