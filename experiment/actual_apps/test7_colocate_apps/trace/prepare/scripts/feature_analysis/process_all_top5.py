import pandas as pd
import matplotlib.pyplot as plt
import os

def main():
    input_file = 'top5_functions_series.csv'
    
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Data file {input_file} not found.")
        return

    # Get all function columns (exclude 'Minute')
    func_cols = [c for c in df.columns if c != 'Minute']
    
    if not func_cols:
        print("No function columns found.")
        return

    print(f"Found {len(func_cols)} functions. Processing...")

    for i, col in enumerate(func_cols):
        # Use index 1-5 for naming
        func_index = i + 1
        print(f"Processing Function {func_index}...")
        
        # 1. Full Data
        df_full = df[['Minute', col]]
        full_csv = f'top5_func_{func_index}_full.csv'
        full_png = f'top5_func_{func_index}_full.png'
        
        df_full.to_csv(full_csv, index=False)
        
        plt.figure(figsize=(15, 6))
        plt.plot(df_full['Minute'], df_full[col], label='RPM', color='tab:blue', linewidth=0.8)
        plt.xlabel('Time (Minutes)')
        plt.ylabel('Invocations per Minute (RPM)')
        plt.title(f'Function {func_index} Full 14 Days RPM\n{col[:30]}...')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig(full_png)
        plt.close()
        
        # 2. First 60 Minutes
        df_60 = df_full.head(60)
        short_csv = f'top5_func_{func_index}_60min.csv'
        short_png = f'top5_func_{func_index}_60min.png'
        
        df_60.to_csv(short_csv, index=False)
        
        plt.figure(figsize=(10, 6))
        plt.plot(df_60['Minute'], df_60[col], label='RPM', color='tab:orange', marker='o', markersize=3)
        plt.xlabel('Time (Minutes)')
        plt.ylabel('Invocations per Minute (RPM)')
        plt.title(f'Function {func_index} First 60 Minutes RPM\n{col[:30]}...')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig(short_png)
        plt.close()
        
        print(f"  Saved: {full_csv}, {full_png}, {short_csv}, {short_png}")

if __name__ == "__main__":
    main()
