import sqlite3

def cleanup_summaries():
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]

    print("🧹 Cleaning Summary Rows...")
    
    total_deleted = 0
    
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = cursor.fetchall()
        # Look for text columns that might contain 'Total:'
        text_cols = [c[1] for c in cols if 'TEXT' in c[2].upper()]
        
        for col in text_cols:
            query = f"DELETE FROM {table} WHERE \"{col}\" LIKE 'Total:%' OR \"{col}\" = 'Total'"
            cursor.execute(query)
            if cursor.rowcount > 0:
                print(f"  - {table}: Deleted {cursor.rowcount} rows (matched col: {col})")
                total_deleted += cursor.rowcount
                
    conn.commit()
    print(f"✨ Finished. Deleted {total_deleted} summary rows across all tables.")
    conn.close()

if __name__ == "__main__":
    cleanup_summaries()
