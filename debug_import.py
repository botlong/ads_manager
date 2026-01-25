import os
import pandas as pd
import glob
import sqlite3

BASE_DIR = r'd:\ads_manager\ads-date\ads-date'
FOLDER = 'products'

def normalize_col(name):
    name = str(name).strip()
    return name.replace(' ', '_').lower()

def debug_import():
    folder_path = os.path.join(BASE_DIR, FOLDER)
    files = glob.glob(os.path.join(folder_path, '*.csv'))
    
    print(f"DTO: {len(files)} files in {FOLDER}")
    
    file_path = files[0]
    print(f"Testing {os.path.basename(file_path)}")
    
    try:
        # Try primary method
        df = pd.read_csv(file_path, skiprows=2, encoding='utf-16', sep=',')
        print(f"Method 1 (utf-16, comma): Success. Cols: {len(df.columns)}")
        
        # Try insert
        conn = sqlite3.connect(r'd:\ads_manager\ads_data.sqlite')
        try:
             df.to_sql('product_debug', conn, if_exists='replace', index=False)
             print("✅ Inserted into product_debug")
             conn.commit()
        except Exception as e:
             print(f"❌ Insert Failed: {e}")
        conn.close()
    except Exception as e:
        print(f"Method 1 Failed: {e}")
        
    try:
        # Try tab
        df = pd.read_csv(file_path, skiprows=2, encoding='utf-16', sep='\t')
        print(f"Method 2 (utf-16, tab): Success. Cols: {len(df.columns)}")
        print(f"Head: {df.columns.tolist()[:3]}")
    except Exception as e:
        print(f"Method 2 Failed: {e}")

if __name__ == "__main__":
    debug_import()
