import sqlite3
import os

DB_PATH = 'ads_data.sqlite'
def check():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(product)")
        cols = cursor.fetchall()
        print(f"Product Cols: {[c[1] for c in cols][:5]}")
    except:
        print("Product table not found")
    conn.close()

if __name__ == "__main__":
    check()
