import sqlite3

def check_order():
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(campaign)")
    cols = cursor.fetchall()
    print(" cid | name | type")
    print("-----|------|-----")
    for col in cols:
        print(f" {col[0]:3d} | {col[1]:<15} | {col[2]}")
    conn.close()

if __name__ == "__main__":
    check_order()
