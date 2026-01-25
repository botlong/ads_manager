import sqlite3
import os

DB_PATH = 'ads_data.sqlite'
if not os.path.exists(DB_PATH):
    print("No DB")
else:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()  
    print(f"COUNT: {len(tables)}")
    for t in tables:
        print(f"- {t[0]}")
    conn.close()
