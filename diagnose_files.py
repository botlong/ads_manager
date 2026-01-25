import pandas as pd
import os
import sys

# Force output encoding
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

FILES_TO_TEST = [
    r'd:\ads_manager\ads-date\ads-date\campaigns\2026.1.10.csv',
    r'd:\ads_manager\ads-date\ads-date\products\2026.1.10.csv'
]

def test_read(path):
    print(f"\nTesting: {os.path.basename(path)}")
    if not os.path.exists(path):
        print("❌ File not found")
        return

    # Method 1: utf-16, comma, skip 2
    try:
        df = pd.read_csv(path, skiprows=2, encoding='utf-16', sep=',')
        print(f"  Method 1 (utf-16, comma): Success. Rows: {len(df)}, Cols: {len(df.columns)}")
        if len(df.columns) < 2:
            print("  ⚠️  Suspicious column count < 2")
    except Exception as e:
        print(f"  Method 1 Failed: {e}")

    # Method 2: utf-16, tab, skip 2
    try:
        df = pd.read_csv(path, skiprows=2, encoding='utf-16', sep='\t')
        print(f"  Method 2 (utf-16, tab): Success. Rows: {len(df)}, Cols: {len(df.columns)}")
    except Exception as e:
        print(f"  Method 2 Failed: {e}")

    # Method 3: utf-8, comma, skip 2
    try:
        df = pd.read_csv(path, skiprows=2, encoding='utf-8', sep=',')
        print(f"  Method 3 (utf-8, comma): Success. Rows: {len(df)}, Cols: {len(df.columns)}")
    except Exception as e:
        print(f"  Method 3 Failed: {e}")

if __name__ == "__main__":
    for f in FILES_TO_TEST:
        test_read(f)
