import os
import glob
import pandas as pd

BASE_DIR = r'd:\ads_manager\ads-date\ads-date'

def inspect_folders():
    folders = [f for f in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, f))]
    
    print(f"Found {len(folders)} folders.")
    
    for folder in folders:
        folder_path = os.path.join(BASE_DIR, folder)
        files = glob.glob(os.path.join(folder_path, '*.csv'))
        
        if not files:
            print(f"⚠️  {folder}: No CSV files")
            continue
            
        # Pick first file
        file_path = files[0]
        try:
            # Try utf-16 first as hinted by error, then utf-8
            try:
                df = pd.read_csv(file_path, encoding='utf-16', sep='\t', nrows=0) # Google Ads often uses utf-16 + tab
            except:
                 try:
                    df = pd.read_csv(file_path, encoding='utf-16le', sep='\t', nrows=0)
                 except:
                    df = pd.read_csv(file_path, encoding='utf-8', nrows=0) # Fallback
            
            headers = list(df.columns)
            print(f"📂 {folder} -> Headers: {headers[:5]} ... ({len(headers)} cols)")
            
        except Exception as e:
            print(f"❌ {folder}: Error reading {os.path.basename(file_path)} - {e}")

if __name__ == "__main__":
    inspect_folders()
