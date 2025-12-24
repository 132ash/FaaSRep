import pandas as pd
import matplotlib.pyplot as plt

def main():
    try:
        df = pd.read_csv('top5_functions_series.csv')
    except FileNotFoundError:
        print("Data file not found. Please run trace_analysis.py first.")
        return

    plt.figure(figsize=(15, 8))
    
    # The first column is 'Minute', others are functions
    x = df['Minute']
    
    # Plot each function
    # Since column names are long hashes, maybe we can shorten them for the legend
    for col in df.columns:
        if col == 'Minute':
            continue
        
        # Shorten label for display
        # Format: Owner_App_Func
        parts = col.split('_')
        if len(parts) >= 3:
            # Take first 6 chars of each hash
            short_label = f"{parts[0][:6]}.._{parts[1][:6]}.._{parts[2][:6]}.."
        else:
            short_label = col[:20] + "..."
            
        plt.plot(x, df[col], label=short_label, linewidth=1, alpha=0.7)
        
    plt.xlabel('Time (Minutes)')
    plt.ylabel('Invocations per Minute (RPM)')
    plt.title('Top 5 HTTP Functions RPM over 14 Days')
    plt.legend(loc='upper right')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    
    output_file = 'top5_functions_rpm.png'
    plt.savefig(output_file)
    print(f"Plot saved to {output_file}")

if __name__ == "__main__":
    main()
