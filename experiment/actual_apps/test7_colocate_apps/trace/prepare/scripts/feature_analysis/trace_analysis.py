import pandas as pd
import glob
import os

def main():
    # 1. Find all csv files
    files = sorted(glob.glob('invocations_per_function_md.anon.d*.csv'))
    if not files:
        print("No csv files found.")
        return

    print(f"Found {len(files)} files.")

    # Dictionary to store total invocations per function
    # Key: (HashOwner, HashApp, HashFunction)
    # Value: Total Invocations
    function_totals = {}

    # Columns representing minutes
    minute_cols = [str(i) for i in range(1, 1441)]

    print("Calculating total invocations...")
    for f in files:
        print(f"Processing {f}...")
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
        
        # Filter Trigger == 'http'
        df_http = df[df['Trigger'] == 'http'].copy()
        
        if df_http.empty:
            continue

        # Calculate sum of invocations for each function in this file
        df_http['daily_sum'] = df_http[minute_cols].sum(axis=1)
        
        # Iterate and update totals
        for _, row in df_http.iterrows():
            func_id = (row['HashOwner'], row['HashApp'], row['HashFunction'])
            function_totals[func_id] = function_totals.get(func_id, 0) + row['daily_sum']

    if not function_totals:
        print("No http functions found.")
        return

    # Calculate Average RPM
    # Total minutes = 14 days * 1440 minutes = 20160
    total_minutes = 14 * 1440
    
    # Convert to list for sorting
    # (func_id, total_invocations, avg_rpm)
    func_stats = []
    for func_id, total in function_totals.items():
        avg_rpm = total / total_minutes
        func_stats.append({'func_id': func_id, 'total': total, 'avg_rpm': avg_rpm})
    
    # Sort by avg_rpm descending
    func_stats.sort(key=lambda x: x['avg_rpm'], reverse=True)
    
    # Get Top 5
    top5 = func_stats[:5]
    print("Top 5 functions by Average RPM:")
    for i, item in enumerate(top5):
        print(f"{i+1}. {item['func_id']} - Avg RPM: {item['avg_rpm']:.4f}")

    top5_ids = set(item['func_id'] for item in top5)

    # Now extract time series data for these top 5 functions
    print("Extracting time series data for Top 5 functions...")
    
    # Structure to hold time series: {func_id: [val1, val2, ...]}
    top5_series = {func_id: [] for func_id in top5_ids}
    
    for f in files:
        print(f"Extracting from {f}...")
        try:
            df = pd.read_csv(f)
        except:
            continue
            
        df_http = df[df['Trigger'] == 'http'].copy()
        
        # Set MultiIndex for easier lookup
        df_http.set_index(['HashOwner', 'HashApp', 'HashFunction'], inplace=True)
        
        for func_id in top5_ids:
            if func_id in df_http.index:
                row = df_http.loc[func_id]
                # Handle case where multiple rows might exist (though unlikely for unique ID)
                if isinstance(row, pd.DataFrame):
                    vals = row[minute_cols].sum(axis=0).values.tolist()
                else:
                    vals = row[minute_cols].values.tolist()
                top5_series[func_id].extend(vals)
            else:
                # Function not found in this day, append 0s
                top5_series[func_id].extend([0] * 1440)

    # Save to CSV for plotting
    output_data = {}
    # Check length
    if not top5_series:
        print("No data extracted.")
        return

    series_len = len(next(iter(top5_series.values())))
    output_data['Minute'] = range(1, series_len + 1)
    
    for func_id, series in top5_series.items():
        # Convert tuple id to string for column name
        # Use a shorter name or the full hash? Full hash is safer.
        col_name = f"{func_id[0]}_{func_id[1]}_{func_id[2]}"
        output_data[col_name] = series
        
    df_out = pd.DataFrame(output_data)
    df_out.to_csv('top5_functions_series.csv', index=False)
    print("Saved time series data to top5_functions_series.csv")

if __name__ == "__main__":
    main()
