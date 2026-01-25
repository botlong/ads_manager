import sqlite3
import shutil
from datetime import datetime

print("=" * 70)
print("ASSET TABLE RECOVERY")
print("=" * 70)

# Backup current database first
current_backup = f"ads_data_before_recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite"
print(f"\n1. Creating safety backup: {current_backup}")
shutil.copy2('ads_data.sqlite', current_backup)
print(f"   [OK] Backup created")

# Source backup file
source_backup = 'ads_data_backup_20260123_123207.sqlite'
print(f"\n2. Source backup: {source_backup}")

# Check source data
print("   Checking source backup...")
conn_source = sqlite3.connect(source_backup)
cursor_source = conn_source.cursor()
source_count = cursor_source.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"   Source has {source_count} rows in asset table")

# Get all asset data from backup
asset_data = cursor_source.execute("SELECT * FROM asset").fetchall()
columns = [desc[0] for desc in cursor_source.description]
conn_source.close()

# Current database
print(f"\n3. Restoring to current database...")
conn_current = sqlite3.connect('ads_data.sqlite')
cursor_current = conn_current.cursor()

# Check current count
current_count = cursor_current.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"   Current asset table has {current_count} rows")

# Clear current asset table
print(f"   Deleting current asset data...")
cursor_current.execute("DELETE FROM asset")
conn_current.commit()
print(f"   [OK] Cleared asset table")

# Insert data from backup
print(f"   Inserting {len(asset_data)} rows from backup...")
placeholders = ','.join(['?' for _ in columns])
insert_sql = f"INSERT INTO asset ({','.join(columns)}) VALUES ({placeholders})"

cursor_current.executemany(insert_sql, asset_data)
conn_current.commit()

# Verify
new_count = cursor_current.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
print(f"   [OK] Inserted {new_count} rows")

conn_current.close()

print("\n" + "=" * 70)
print("RECOVERY COMPLETE!")
print(f"Asset table restored: {current_count} -> {new_count} rows")
print(f"Safety backup saved as: {current_backup}")
print("=" * 70)
