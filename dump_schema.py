import sqlite3

def dump_schema():
    conn = sqlite3.connect('ads_data.sqlite')
    cursor = conn.cursor()
    
    with open('db_schema.txt', 'w', encoding='utf-8') as f:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            f.write(f"\nTable: {table_name}\n")
            
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            for col in columns:
                f.write(f"  - {col[1]} ({col[2]})\n")
                
    conn.close()

if __name__ == "__main__":
    dump_schema()
