import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ads_data.sqlite')

def init_prefs_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create user_preferences table
    # We use table_name + item_identifier to track preferences for any table
    # item_identifier will be the primary key value of the row (e.g., campaign name)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            table_name TEXT,
            item_identifier TEXT,
            is_pinned INTEGER DEFAULT 0,
            display_order INTEGER DEFAULT 0,
            PRIMARY KEY (table_name, item_identifier)
        )
    """)
    
    conn.commit()
    conn.close()
    print("user_preferences table created successfully.")

if __name__ == "__main__":
    init_prefs_db()
