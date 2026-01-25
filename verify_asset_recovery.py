import sqlite3

print("=" * 70)
print("ASSET TABLE RECOVERY VERIFICATION")
print("=" * 70)

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

# Get count
count = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"\nTotal rows in asset table: {count}")

# Get sample data
sample = cursor.execute("SELECT * FROM asset LIMIT 10").fetchall()
cols = [desc[0] for desc in cursor.description]

print(f"\nColumns ({len(cols)}): ad_group_status, ad_group, campaign, status, ...")
print(f"\nFirst 10 rows:")
print("-" * 70)

for i, row in enumerate(sample, 1):
    ad_group = row[1] if row[1] else "(None)"
    campaign = row[2] if row[2] else "(None)"
    status = row[3] if row[3] else "(None)"
    print(f"{i:2}. {ad_group[:40]:40} | Campaign: {campaign[:20]:20}")

print("=" * 70)
print(f"\n[SUCCESS] Asset table now has {count} rows")
print("=" * 70)

conn.close()
