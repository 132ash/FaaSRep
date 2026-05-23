import os
import glob

def clean_data(base_dir, threshold=40):
    # Define directories
    csv_dir = os.path.join(base_dir, 'function_csvs')
    plot_dir = os.path.join(base_dir, 'function_plots')
    
    dirs_to_clean = [csv_dir, plot_dir]
    
    print(f"Cleaning files with rank > {threshold} in subdirectories of {base_dir}...")
    
    deleted_count = 0
    
    for d in dirs_to_clean:
        if not os.path.exists(d):
            print(f"Directory not found: {d}")
            continue
            
        # Get all files starting with 'rank_'
        # Matches both .csv and .png
        files = glob.glob(os.path.join(d, 'rank_*'))
        
        for f in files:
            filename = os.path.basename(f)
            try:
                # Filename format: rank_0001_avgRPM_...
                # Split by '_' to get parts
                parts = filename.split('_')
                
                # Check if it matches the expected format
                if len(parts) > 1 and parts[0] == 'rank':
                    # The second part (index 1) should be the rank number
                    rank = int(parts[1])
                    
                    if rank > threshold:
                        os.remove(f)
                        deleted_count += 1
            except ValueError:
                # If int conversion fails, skip
                continue
            except Exception as e:
                print(f"Error deleting {filename}: {e}")
                
    print(f"Cleanup complete. Deleted {deleted_count} files.")

if __name__ == "__main__":
    # Use the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    clean_data(script_dir, threshold=40)
