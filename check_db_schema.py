import sqlite3
import os

DB_PATH = 'ads_data.sqlite'

def check_schema():
    if not os.path.exists(DB_PATH):
        print(f"File not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    
    for t in tables:
        cursor.execute(f"PRAGMA table_info({t})")
        cols = [r[1] for r in cursor.fetchall()]
        print(f"{t}: {cols}")
        
        # Also check some data from product if it exists
        if t == 'product':
            cursor.execute("SELECT * FROM product LIMIT 3")
            rows = cursor.fetchall()
            print(f"Sample data from {t}:")
            for r in rows:
                print(r)
    
    conn.close()

if __name__ == "__main__":
    check_schema()
