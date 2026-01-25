import sqlite3

DB_PATH = 'ads_data.sqlite'

def add_indexes():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🚀 Adding Indexes for Performance Optimization...")
    
    # List of tables to index by campaign + date
    tables_with_campaign = [
        'campaign',
        'search_term',
        'asset',
        'audience',
        'age',
        'gender',
        'location_by_cities_all_campaign',
        'ad_schedule',
        'keyword'
    ]
    
    # 1. Campaign & Date Indexes
    for table in tables_with_campaign:
        try:
            print(f"  - Indexing {table}...")
            # Create composite index if table has both columns
            # Check columns first
            cursor.execute(f"PRAGMA table_info({table})")
            cols = [c[1] for c in cursor.fetchall()]
            
            if 'campaign' in cols and 'date' in cols:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_camp_date ON {table}(campaign, date)")
                print(f"    ✓ Added idx_{table}_camp_date")
            elif 'campaigns' in cols and 'date' in cols: # Handle 'channel' table if needed, though usually 'campaigns'
                 cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_camp_date ON {table}(campaigns, date)")
                 print(f"    ✓ Added idx_{table}_camp_date (using 'campaigns' col)")
            else:
                print(f"    ⚠ Skipped (Missing columns)")
                
        except Exception as e:
            print(f"    ❌ Error: {e}")

    # 2. Channel Table Special Handling (usually 'campaigns' plural)
    try:
        print(f"  - Indexing channel...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_channel_camp_date ON channel(campaigns, date)")
        print(f"    ✓ Added idx_channel_camp_date")
    except Exception as e:
        print(f"    ❌ Error indexing channel: {e}")

    # 3. Product Table (Date only)
    try:
        print(f"  - Indexing product...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_date ON product(date)")
        print(f"    ✓ Added idx_product_date")
    except Exception as e:
         print(f"    ❌ Error indexing product: {e}")

    conn.commit()
    conn.close()
    print("✨ Indexing Complete!")

if __name__ == "__main__":
    add_indexes()
