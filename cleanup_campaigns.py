import sqlite3
import os

DB_PATH = 'ads_data.sqlite'

def cleanup():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check counts before
    cursor.execute("SELECT count(*) FROM campaign")
    total_before = cursor.fetchone()[0]
    
    cursor.execute("SELECT count(*) FROM campaign WHERE campaign_status = 'Enabled'")
    enabled_count = cursor.fetchone()[0]
    
    print(f"Before: {total_before} rows. (Enabled: {enabled_count})")
    
    # Delete
    cursor.execute("DELETE FROM campaign WHERE campaign_status != 'Enabled'")
    deleted = cursor.rowcount
    conn.commit()
    
    # Check counts after
    cursor.execute("SELECT count(*) FROM campaign")
    total_after = cursor.fetchone()[0]
    
    print(f"Deleted {deleted} rows.")
    print(f"After: {total_after} rows.")
    
    conn.close()

if __name__ == "__main__":
    cleanup()
