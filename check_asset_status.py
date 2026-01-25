import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

print("=" * 70)
print("ASSET TABLE STATUS CHECK")
print("=" * 70)

# Get total count
total = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"\nTotal rows in asset table: {total}")

# Get status distribution
print("\nad_group_status distribution:")
print("-" * 70)
status_counts = cursor.execute("""
    SELECT ad_group_status, COUNT(*) as count
    FROM asset
    GROUP BY ad_group_status
    ORDER BY count DESC
""").fetchall()

for status, count in status_counts:
    status_display = status if status else "(NULL)"
    print(f"  {status_display:30} : {count:3} rows")

# Count rows that will be deleted (case-insensitive check)
to_delete = cursor.execute("""
    SELECT COUNT(*) FROM asset 
    WHERE LOWER(ad_group_status) != 'enabled' OR ad_group_status IS NULL
""").fetchone()[0]

to_keep = cursor.execute("""
    SELECT COUNT(*) FROM asset 
    WHERE LOWER(ad_group_status) = 'enabled'
""").fetchone()[0]

print("\n" + "=" * 70)
print(f"Rows with 'Enabled' status (to keep): {to_keep}")
print(f"Rows with other status (to delete): {to_delete}")
print("=" * 70)

conn.close()
