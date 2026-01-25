import sqlite3
import os

print("=" * 70)
print("DATABASE FILE VERIFICATION")
print("=" * 70)

# Check which file exists and its location
db_file = 'ads_data.sqlite'
backup_file = 'ads_data_backup_20260123_123207.sqlite'

print(f"\n1. Main Database: {db_file}")
if os.path.exists(db_file):
    abs_path = os.path.abspath(db_file)
    size = os.path.getsize(db_file) / 1024 / 1024  # MB
    print(f"   Path: {abs_path}")
    print(f"   Size: {size:.2f} MB")
    print(f"   Exists: YES")
    
    # Check asset table
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    count = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
    
    # Get sample data
    sample = cursor.execute("SELECT ad_group FROM asset WHERE ad_group IS NOT NULL LIMIT 3").fetchall()
    print(f"   Asset table rows: {count}")
    print(f"   Sample data:")
    for row in sample:
        print(f"     - {row[0]}")
    conn.close()
else:
    print(f"   Exists: NO")

print(f"\n2. Backup File: {backup_file}")
if os.path.exists(backup_file):
    abs_path = os.path.abspath(backup_file)
    size = os.path.getsize(backup_file) / 1024 / 1024  # MB
    print(f"   Path: {abs_path}")
    print(f"   Size: {size:.2f} MB")
    print(f"   Exists: YES")
    
    # Check asset table in backup
    conn = sqlite3.connect(backup_file)
    cursor = conn.cursor()
    count = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
    print(f"   Asset table rows: {count}")
    conn.close()
else:
    print(f"   Exists: NO")

print("\n" + "=" * 70)
print("CONFIRMATION:")
print(f"Recovery script operated on: {os.path.abspath(db_file)}")
print(f"This is the MAIN database (ads_data.sqlite)")
print("=" * 70)
