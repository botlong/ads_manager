import sqlite3

def inspect_db():
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("=== Database Schema ===")
    for table in tables:
        table_name = table[0]
        print(f"\nTable: {table_name}")
        
        # Get columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
            
    conn.close()

if __name__ == "__main__":
    inspect_db()
