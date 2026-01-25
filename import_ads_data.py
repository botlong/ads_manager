import os
import sqlite3
import pandas as pd
import glob
import re
import sys

# Force utf-8 output to avoid console crashes
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

BASE_DIR = r'd:\ads_manager\ads-date\ads-date'
DB_PATH = 'ads_data.sqlite'

FOLDER_MAP = {
    'campaigns': 'campaign',
    'terms': 'search_term',
    'products': 'product',
    'locations': 'location_by_cities_all_campaign',
    'ad schedule': 'ad_schedule',
    'age': 'age',
    'gender': 'gender',
    'asset': 'asset',
    'channel': 'channel',
    'keywords': 'keyword' 
}

def safe_print(msg):
    try:
        print(msg)
    except:
        try:
            print(msg.encode('utf-8', errors='replace').decode('utf-8'))
        except:
             pass

def normalize_col(name):
    """Normalize column name to snake_case."""
    name = str(name).strip()
    name = name.replace('%', '_percent')
    name = name.replace('.', '')
    name = name.replace('/', '_')
    name = name.replace('(', '')
    name = name.replace(')', '')
    name = name.replace(' ', '_')
    name = name.replace('-', '_')
    return name.lower()

def parse_date(filename):
    """Extract date from filename 2026.1.10.csv -> 2026-01-10"""
    basename = os.path.basename(filename)
    name = os.path.splitext(basename)[0] # 2026.1.10
    parts = name.split('.')
    if len(parts) == 3:
        y, m, d = parts
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None

def import_data():
    conn = sqlite3.connect(DB_PATH)
    
    folders = [f for f in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, f))]
    
    safe_print(f"🚀 Starting import from {len(folders)} folders...")
    
    for folder in folders:
        # Determine table name
        table_name = FOLDER_MAP.get(folder.lower(), folder.lower().replace(' ', '_'))
        folder_path = os.path.join(BASE_DIR, folder)
        files = glob.glob(os.path.join(folder_path, '*.csv'))
        
        if not files:
            safe_print(f"⚠️  Skipping {folder}: No CSV files")
            continue
            
        safe_print(f"\n📂 Processing {folder} -> Table: {table_name}")
        
        all_dfs = []
        
        for file_path in files:
            date_str = parse_date(file_path)
            if not date_str:
                safe_print(f"  ❌ Skipping {os.path.basename(file_path)}: Cannot parse date")
                continue
            
            # Auto-detect format
            df = None
            detected_success = False
            
            # Formats to try: prioritize skip=0 (standard CSV)
            formats = [
                (0, ',', 'utf-16'), (0, '\t', 'utf-16'),
                (2, ',', 'utf-16'), (2, '\t', 'utf-16'),
                (1, ',', 'utf-16'), (1, '\t', 'utf-16'),
                (0, ',', 'utf-8'), (2, ',', 'utf-8')
            ]
            
            for skip, sep, enc in formats:
                try:
                    temp_df = pd.read_csv(file_path, skiprows=skip, sep=sep, encoding=enc, nrows=5)
                    if len(temp_df.columns) > 1:
                        # Success candidate! Read full file
                        df = pd.read_csv(file_path, skiprows=skip, sep=sep, encoding=enc)
                        detected_success = True
                        break
                except:
                    continue
            
            if not detected_success or df is None:
                 safe_print(f"  ❌ Failed to detect format for {os.path.basename(file_path)}")
                 continue
            
            # safe_print(f"  + Read {os.path.basename(file_path)}: {len(df)} rows")

            try:
                # Normalize columns
                df.columns = [normalize_col(c) for c in df.columns]
                
                # Add Date as FIRST column
                df.insert(0, 'date', date_str)
                
                # Handle day_and_time removal if present
                if 'day_and_time' in df.columns:
                    df = df.drop(columns=['day_and_time'])
                
                all_dfs.append(df)
                
            except Exception as e:
                safe_print(f"  ❌ Failed to process {os.path.basename(file_path)}: {e}")

        if not all_dfs:
            continue
            
        # Concatenate all data for this table
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # Create Table / Insert
        # using if_exists='replace' only for first batch? 
        # Actually since we start fresh, 'replace' is fine for the whole batch concat.
        
        try:
            final_df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.commit()
            safe_print(f"  ✅ Imported {len(final_df)} rows into {table_name}")
            
            # Verify columns
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            cols = cursor.fetchall()
            safe_print(f"     Schema: {[c[1] for c in cols][:5]}...")
            
        except Exception as e:
             safe_print(f"  ❌ Failed to save table {table_name}: {e}")

    conn.close()
    safe_print("\n🎉 Import Data Complete!")

if __name__ == "__main__":
    import_data()
