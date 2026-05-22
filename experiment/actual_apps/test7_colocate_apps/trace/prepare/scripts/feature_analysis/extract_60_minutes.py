import pandas as pd
import matplotlib.pyplot as plt

def main():
    input_file = 'single_function_series.csv'
    output_csv = 'single_function_first_60.csv'
    output_png = 'single_function_first_60.png'

    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Data file {input_file} not found.")
        return

    # Extract first 60 minutes
    df_60 = df.head(60)
    
    # Save to CSV
    df_60.to_csv(output_csv, index=False)
    print(f"Saved first 60 minutes data to {output_csv}")

    # Plot
    plt.figure(figsize=(10, 6))
    # The second column is the value
    target_col = df.columns[1]
    
    plt.plot(df_60['Minute'], df_60[target_col], label='RPM', marker='o', markersize=3)
    
    plt.xlabel('Time (Minutes)')
    plt.ylabel('Invocations per Minute (RPM)')
    plt.title('RPM First 60 Minutes')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    plt.savefig(output_png)
    print(f"Plot saved to {output_png}")

if __name__ == "__main__":
    main()
