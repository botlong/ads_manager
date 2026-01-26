import sqlite3
import os

db_path = 'ads_data.sqlite'
targets = ['search_term', 'channel', 'product', 'location_by_cities_all_campaign', 'keyword', 'age', 'gender', 'time', 'campaign']
output_file = 'schema_log.txt'

with open(output_file, 'w', encoding='utf-8') as f:
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for t in targets:
            f.write(f"--- {t} ---\n")
            try:
                cursor.execute(f"PRAGMA table_info({t})")
                cols = [c[1] for c in cursor.fetchall()]
                f.write(", ".join(cols) + "\n\n")
            except Exception as e:
                f.write(f"ERROR: {e}\n\n")
        conn.close()
    else:
        f.write("DB not found\n")
print(f"Schema written to {output_file}")
