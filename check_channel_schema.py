import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

print("===== CHANNEL TABLE SCHEMA =====")
columns = cursor.execute("PRAGMA table_info(channel)").fetchall()
for col in columns:
    print(f"  {col[1]:30} ({col[2]})")

print(f"\nTotal columns: {len(columns)}")

# Check for 'campaign' vs 'campaigns'
col_names = [col[1] for col in columns]
if 'campaign' in col_names:
    print("\n✓ 'campaign' column EXISTS")
else:
    print("\n✗ 'campaign' column NOT FOUND")
    
if 'campaigns' in col_names:
    print("✓ 'campaigns' column EXISTS")
else:
    print("✗ 'campaigns' column NOT FOUND")

print("\n===== SAMPLE DATA =====")
sample = cursor.execute("SELECT * FROM channel LIMIT 3").fetchall()
print(f"Found {len(sample)} rows")

conn.close()
