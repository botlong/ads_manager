import sqlite3
import shutil
from datetime import datetime

print("=" * 70)
print("ASSET TABLE CLEANUP BY STATUS")
print("=" * 70)

conn = sqlite3.connect('ads_data.sqlite')
cursor = conn.cursor()

# Step 1: Check current status
print("\nStep 1: Current status distribution")
print("-" * 70)
status_counts = cursor.execute("""
    SELECT ad_group_status, COUNT(*) as count
    FROM asset
    GROUP BY ad_group_status
""").fetchall()

for status, count in status_counts:
    print(f"  {status}: {count} rows")

total = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"\nTotal rows: {total}")

# Step 2: Create backup
backup_file = f"ads_data_before_asset_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite"
print(f"\nStep 2: Creating backup: {backup_file}")
shutil.copy2('ads_data.sqlite', backup_file)
print("  [OK] Backup created")

# Step 3: Delete non-Enabled rows
print("\nStep 3: Deleting non-Enabled rows...")
cursor.execute("""
    DELETE FROM asset 
    WHERE LOWER(ad_group_status) != 'enabled' OR ad_group_status IS NULL
""")
deleted = cursor.rowcount
conn.commit()
print(f"  [OK] Deleted {deleted} rows")

# Step 4: Verify
remaining = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"\nStep 4: Verification")
print(f"  Rows before: {total}")
print(f"  Rows deleted: {deleted}")
print(f"  Rows remaining: {remaining}")

conn.close()

print("\n" + "=" * 70)
print("CLEANUP COMPLETE!")
print(f"Backup saved as: {backup_file}")
print("=" * 70)
