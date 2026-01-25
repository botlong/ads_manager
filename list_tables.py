import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

tables = [t[0] for t in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

print("===== DATABASE TABLES =====")
for table in tables:
    print(f"  {table}")

print(f"\nTotal: {len(tables)} tables")

if 'channels' in tables:
    print("\n✓ 'channels' table EXISTS")
else:
    print("\n✗ 'channels' table NOT FOUND")
    
if 'channel' in tables:
    print("✓ 'channel' table EXISTS")
else:
    print("✗ 'channel' table NOT FOUND")

conn.close()
