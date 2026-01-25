import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

# Get all tables
tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("=== All Tables in Database ===")
for table in tables:
    print(f"  - {table[0]}")

print("\n=== Checking 'channel' table ===")
try:
    columns = cursor.execute("PRAGMA table_info(channel)").fetchall()
    if columns:
        print("channel table EXISTS with columns:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
    else:
        print("channel table does NOT exist")
except Exception as e:
    print(f"Error: {e}")

conn.close()
