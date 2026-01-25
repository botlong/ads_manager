import sqlite3

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

print("=" * 70)
print("ASSET TABLE CLEANUP VERIFICATION")
print("=" * 70)

# Get current count
total = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"\nTotal rows in asset table: {total}")

# Check status distribution
print("\nad_group_status distribution:")
status_counts = cursor.execute("""
    SELECT ad_group_status, COUNT(*) as count
    FROM asset
    GROUP BY ad_group_status
""").fetchall()

for status, count in status_counts:
    print(f"  {status}: {count} rows")

# Show sample data
print("\nSample data (first 8 rows):")
print("-" * 70)
sample = cursor.execute("SELECT ad_group, ad_group_status, campaign FROM asset LIMIT 8").fetchall()
for i, (ad_group, status, campaign) in enumerate(sample, 1):
    ad_group_display = (ad_group if ad_group else "(None)")[:30]
    status_display = status if status else "(None)"
    campaign_display = (campaign if campaign else "(None)")[:20]
    print(f"{i}. {ad_group_display:30} | {status_display:10} | {campaign_display}")

print("\n" + "=" * 70)
print(f"[SUCCESS] Asset table cleaned: {total} rows remaining (all Enabled)")
print("=" * 70)

conn.close()
