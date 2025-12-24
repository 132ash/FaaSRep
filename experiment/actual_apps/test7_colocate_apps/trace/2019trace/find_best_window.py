import pandas as pd
import matplotlib.pyplot as plt

def main():
    input_file = 'top5_func_1_full.csv'
    output_csv = 'top5_func_1_selected_60min.csv'
    output_png = 'top5_func_1_selected_60min.png'
    
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
    min_rpm = 3000
    max_rpm = 6000
    
    best_window_start = -1
    max_score = -float('inf')
    
    # Penalty weight for being out of range
    # We want to encourage high RPM within range, but discourage going out of range.
    # Score function per point:
    # - If valid (1500-4000): score = rpm (Higher is better, closer to 4000)
    # - If low (<1500): score = 1500 - penalty * (1500 - rpm)
    # - If high (>4000): score = 4000 - penalty * (rpm - 4000)
    penalty_weight = 2.0 

    print(f"Searching for 60-min windows maximizing points close to {max_rpm}...")
    print(f"Using weighted score with penalty factor {penalty_weight} for out-of-bounds.")
    
    for i in range(len(df) - window_size + 1):
        window = series.iloc[i : i + window_size]
        
        score = 0
        for val in window:
            if min_rpm <= val <= max_rpm:
                score += val
            elif val < min_rpm:
                score += (min_rpm - penalty_weight * (min_rpm - val))
            else: # val > max_rpm
                score += (max_rpm - penalty_weight * (val - max_rpm))
        
        if score > max_score:
            max_score = score
            best_window_start = i

    if best_window_start != -1:
        best_window = series.iloc[best_window_start : best_window_start + window_size]
        mean_rpm = best_window.mean()
        
        # Count points near 4000 (e.g. > 3000)
        high_points = sum(1 for x in best_window if 3000 <= x <= 4000)
        
        print(f"Best window found at index {best_window_start} (Minute {minutes.iloc[best_window_start]}).")
        print(f"Score: {max_score:.2f}")
        print(f"Mean RPM: {mean_rpm:.2f}")
        print(f"Points between 3000 and 4000: {high_points}")
        
        # Calculate violations for reporting
        violations = 0
        for val in best_window:
            if val < min_rpm: violations += (min_rpm - val)
            elif val > max_rpm: violations += (val - max_rpm)
        print(f"Total violation magnitude: {violations:.2f}")

    else:
        print("Error: No window processed.")
        return

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
    # Min 1500 RPM -> 25 RPS
    # Max 4000 RPM -> 66.67 RPS
    plt.axhline(y=min_rpm/60, color='r', linestyle='--', label=f'Min {min_rpm/60:.1f} RPS')
    plt.axhline(y=max_rpm/60, color='g', linestyle='--', label=f'Max {max_rpm/60:.1f} RPS')
    
    plt.xlabel('Time (Seconds)')
    plt.ylabel('Requests Per Second (RPS)')
    plt.title(f'Selected 60 Minutes Window (RPS)\nOriginal Start Minute: {df["Minute"].iloc[best_window_start]}')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_png)
    print(f"Plot saved to {output_png}")

if __name__ == "__main__":
    main()
