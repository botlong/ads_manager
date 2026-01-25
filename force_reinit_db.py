import sqlite3
import os
import time
import shutil
from datetime import datetime

DB_PATH = 'ads_data.sqlite'

def force_reinit():
    print("🚀 Starting Force DB Reinit")
    
    # 1. Read Schema
    if not os.path.exists(DB_PATH):
        print("❌ Database not found!")
        return
        
    print("📖 Reading schema...")
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall() if row[0] != 'sqlite_sequence']
        
        table_schemas = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            cols_def = []
            for col in columns:
                if col[1] != 'date' and col[1] != 'day_and_time':
                    cols_def.append((col[1], col[2]))
            table_schemas[table] = cols_def
        conn.close()
        print(f"✅ Schema read ({len(tables)} tables)")
    except Exception as e:
        print(f"❌ Error reading schema: {e}")
        return

    # 2. Rename Old DB (simulating delete/backup)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    trash_path = f'ads_data_trash_{timestamp}.sqlite'
    
    print(f"🗑️  Attempting to move old DB to {trash_path}...")
    try:
        if os.path.exists(DB_PATH):
            os.rename(DB_PATH, trash_path)
            print("✅ File moved successfully.")
        else:
            print("⚠️ File gone? Proceeding.")
    except Exception as e:
        print(f"❌ Failed to move file: {e}")
        print("Trying copying and deleting...")
        try:
            shutil.copy2(DB_PATH, trash_path)
            os.remove(DB_PATH)
            print("✅ File copied and deleted.")
        except Exception as e2:
            print(f"❌ Still failed: {e2}")
            print("🛑 ABORTING to avoid partial state.")
            return

    # 3. Create New DB
    print("🔨 Creating new database...")
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for table, cols in table_schemas.items():
            create_parts = []
            # Date First
            create_parts.append("date TEXT")
            create_parts.extend([f"{col[0]} {col[1]}" for col in cols])
            
            create_sql = f"CREATE TABLE {table} ({', '.join(create_parts)})"
            cursor.execute(create_sql)
            print(f"  - Created {table}")
            
        conn.commit()
        conn.close()
        print("✨ Success!")
        
    except Exception as e:
        print(f"❌ Create failed: {e}")

if __name__ == "__main__":
    force_reinit()
