import sqlite3

def inspect_summaries():
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]

    for table in tables:
        print(f"--- {table} ---")
        # Try to find 'Total' in the first text column (usually column 1 or 2, date is 0)
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        
        # Heuristic: check first few text columns for 'Total'
        text_cols = [c[1] for c in cols if 'TEXT' in c[2].upper()]
        
        for col in text_cols:
            try:
                cursor.execute(f"SELECT * FROM {table} WHERE \"{col}\" LIKE 'Total%' LIMIT 2")
                rows = cursor.fetchall()
                if rows:
                    print(f"Found in column '{col}':")
                    for r in rows:
                        print(r)
            except Exception as e:
                print(f"Error checking {col}: {e}")
                
    conn.close()

if __name__ == "__main__":
    inspect_summaries()
