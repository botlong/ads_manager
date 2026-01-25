import sqlite3
import shutil
from datetime import datetime
import os

DB_PATH = 'ads_data.sqlite'

def backup_database():
    """Backup the current database."""
    if not os.path.exists(DB_PATH):
        print(f"❌ Database {DB_PATH} not found.")
        return None
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'ads_data_backup_{timestamp}.sqlite'
    
    print(f"📦 Backing up database to: {backup_path}")
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ Backup complete.\n")
    return backup_path

def reinit_database():
    """Re-initialize database: read schema, drop all, recreate with 'date' column."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 1. Read current schema
        print("🔍 Reading current schema...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
        
        table_schemas = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall() # list of (cid, name, type, notnull, dflt_value, pk)
            # Store as list of (name, type)
            cols_def = []
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                if col_name != 'date': # Avoid duplicate if running multiple times (though we drop tables so mostly fine)
                    cols_def.append((col_name, col_type))
            table_schemas[table] = cols_def
            print(f"  - Found table: {table} ({len(cols_def)} columns)")

            print(f"  - Found table: {table} ({len(cols_def)} columns)")

        conn.close() # Close to allow file deletion
        
        # 2. Delete database file to ensure clean state
        if os.path.exists(DB_PATH):
             try:
                 os.remove(DB_PATH)
                 print(f"\n🗑️  Deleted existing database file: {DB_PATH}")
             except PermissionError:
                 print(f"\n❌ Could not delete file (locked?). Attempting DROP TABLE instead.")
                 conn = sqlite3.connect(DB_PATH) # Reconnect if delete failed
                 cursor = conn.cursor()
                 for table in tables:
                     cursor.execute(f"DROP TABLE IF EXISTS {table}")
                 conn.commit()
                 conn.close()

        # 3. Recreate tables
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print("\n🔨 Recreating tables with 'date' column...")
        
        for table, cols in table_schemas.items():
            # Construct CREATE STATEMENT
            create_parts = []
            # Add date column FIRST
            create_parts.append("date TEXT")
            # Add original columns
            create_parts.extend([f"{col[0]} {col[1]}" for col in cols])
            
            create_sql = f"CREATE TABLE {table} ({', '.join(create_parts)})"
            cursor.execute(create_sql)
            print(f"  - Recreated {table} with date column (FIRST)")
            
        conn.commit()
        print("\n✨ Database re-initialization complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    print("="*60)
    print("🔄 Database Re-initialization Tool (Add Date Column)")
    print("="*60 + "\n")
    
    if backup_database():
        confirm = input("❓ Confirm DELETE ALL DATA and re-init schema? (Type 'YES'): ")
        if confirm == 'YES':
            reinit_database()
        else:
            print("❌ Operation cancelled.")
