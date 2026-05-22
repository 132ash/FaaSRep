import pandas as pd
import matplotlib.pyplot as plt

def main():
    input_file = 'top5_functions_series.csv'
    target_col = '104f438d72947b49216b97034e44c8f80e90b58c41c08afdad10f25a6eb1af7d_ec941895b5f5b86d440292b4230223dfdc89f1822575661d780c1dffb54c720d_9f7c42b1b5e58255e47691a7b28b08272a9e6a7db596871d300d5ed6a0363e35'
    output_csv = 'single_function_series.csv'
    output_png = 'single_function_rpm.png'

    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Data file {input_file} not found.")
        return

    if target_col not in df.columns:
        print(f"Column {target_col} not found in {input_file}")
        print("Available columns:", df.columns.tolist())
        return

    # Extract data
    df_subset = df[['Minute', target_col]]
    
    # Save to CSV
    df_subset.to_csv(output_csv, index=False)
    print(f"Saved extracted data to {output_csv}")

    # Plot
    plt.figure(figsize=(15, 6))
    plt.plot(df_subset['Minute'], df_subset[target_col], label='RPM', color='blue', linewidth=1)
    
    plt.xlabel('Time (Minutes)')
    plt.ylabel('Invocations per Minute (RPM)')
    plt.title(f'RPM over 14 Days for Function\n{target_col[:20]}...{target_col[-20:]}')
    plt.legend()
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    
    plt.savefig(output_png)
    print(f"Plot saved to {output_png}")

if __name__ == "__main__":
    main()
