import sqlite3
import os
import glob
from datetime import datetime

print("=" * 60)
print("ASSET TABLE STATUS CHECK")
print("=" * 60)

# Check current database
print("\n1. CURRENT DATABASE (ads_data.sqlite)")
print("-" * 60)
try:
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    
    count = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
    print(f"   Total rows: {count}")
    
    if count > 0:
        # Get a sample
        sample = cursor.execute("SELECT * FROM asset LIMIT 3").fetchall()
        cols = [desc[0] for desc in cursor.description]
        print(f"   Columns ({len(cols)}): {', '.join(cols[:5])}...")
        print(f"   Sample data (first 3 rows):")
        for i, row in enumerate(sample, 1):
            print(f"     Row {i}: {str(row)[:80]}...")
    else:
        print("   ⚠️  TABLE IS EMPTY!")
    
    conn.close()
except Exception as e:
    print(f"   ERROR: {e}")

# Check backups
print("\n2. BACKUP FILES")
print("-" * 60)
backups = sorted(glob.glob("ads_data_backup_*.sqlite"), reverse=True)

if not backups:
    print("   ⚠️  NO BACKUPS FOUND!")
else:
    print(f"   Found {len(backups)} backup file(s):\n")
    
    for i, backup in enumerate(backups[:3], 1):  # Check first 3 backups
        size = os.path.getsize(backup) / 1024 / 1024  # MB
        mtime = datetime.fromtimestamp(os.path.getmtime(backup))
        
        print(f"   {i}. {backup}")
        print(f"      Size: {size:.2f} MB")
        print(f"      Date: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check asset table in backup
        try:
            conn = sqlite3.connect(backup)
            cursor = conn.cursor()
            backup_count = cursor.execute("SELECT COUNT(*) FROM asset").fetchone()[0]
            print(f"      Asset rows: {backup_count}")
            conn.close()
        except Exception as e:
            print(f"      ERROR reading backup: {e}")
        print()

print("=" * 60)
