import os
import sqlite3
import pandas as pd
import glob
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

BASE_DIR = r'd:\ads_manager\ads-date\ads-date'
DB_PATH = 'ads_data.sqlite'

def normalize_col(name):
    name = str(name).strip()
    return name.replace(' ', '_').replace('.', '').replace('-', '_').lower()

def parse_date(filename):
    basename = os.path.basename(filename)
    name = os.path.splitext(basename)[0]
    parts = name.split('.')
    if len(parts) == 3:
        y, m, d = parts
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None

def import_products():
    folder = 'products'
    folder_path = os.path.join(BASE_DIR, folder)
    files = glob.glob(os.path.join(folder_path, '*.csv'))
    
    print(f"Products Import: Found {len(files)} files.")
    
    conn = sqlite3.connect(DB_PATH)
    all_dfs = []
    
    for file_path in files:
        date_str = parse_date(file_path)
        try:
            # Try skiprows=2, utf-16, TAB
            df = pd.read_csv(file_path, skiprows=2, encoding='utf-16', sep='\t')
            
            # Normalize
            df.columns = [normalize_col(c) for c in df.columns]
            df.insert(0, 'date', date_str)
            if 'day_and_time' in df.columns:
                 df.drop(columns=['day_and_time'], inplace=True)
            
            all_dfs.append(df)
            print(f"  + Read {os.path.basename(file_path)}: {len(df)} rows")
        except Exception as e:
            print(f"  ❌ Failed {os.path.basename(file_path)}: {e}")
            
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_sql('product', conn, if_exists='replace', index=False)
        conn.commit()
        print(f"✅ Imported {len(final_df)} rows into 'product'")
    else:
        print("❌ No data collected for products.")
        
    conn.close()

if __name__ == "__main__":
    import_products()
