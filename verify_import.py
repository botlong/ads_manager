import sqlite3
import pandas as pd

DB_PATH = 'ads_data.sqlite'

def verify_db():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    
    print(f"found {len(tables)} tables: {tables}\n")
    
    for table in tables:
        # Check Count
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        
        # Check Schema
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        col_names = [c[1] for c in cols]
        
        status = "✅"
        msg = []
        
        if count == 0:
            status = "⚠️"
            msg.append("Empty")
        
        if col_names[0] != 'date':
            status = "❌"
            msg.append(f"Date not first (is {col_names[0]})")
            
        if 'day_and_time' in col_names:
             status = "❌"
             msg.append("Has day_and_time")
             
        print(f"{status} {table:<30} Rows: {count:<6} Cols: {len(cols):<3} {', '.join(msg)}")

    conn.close()

if __name__ == "__main__":
    import os
    verify_db()
