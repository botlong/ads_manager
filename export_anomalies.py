
import sqlite3
import pandas as pd
import os

DB_PATH = 'd:/ads_manager/ads_data.sqlite'
OUTPUT_FILE = 'd:/ads_manager/anomalies_report.xlsx'

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def export_anomalies():
    print("Starting Native Format Anomaly Export...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get all campaigns (only from search_term as product table lacks campaign column)
    cursor.execute("SELECT DISTINCT campaign FROM search_term")
    # Also fetch campaigns from main campaign table to be safe
    cursor.execute("SELECT DISTINCT campaign FROM campaign")
    campaigns = [row[0] for row in cursor.fetchall() if row[0]]
    
    # Remove duplicates
    campaigns = list(set(campaigns))
    
    tables_map = {
        'campaign': 'Campaign Overview',
        'search_term': 'Search Term',
        'product': 'Product', 
        'channel': 'Channel',
        'asset': 'Asset',
        'audience': 'Audience',
        'location_by_cities_all_campaign': 'Location',
        'age': 'Age',
        'gender': 'Gender',
        'ad_schedule': 'Ad Schedule'
    }

    # Dictionary to hold DataFrame for each sheet
    sheets_data = {}
    
    skipped_tables = []

    try:
        for table, legible_name in tables_map.items():
            print(f"Checking table: {table}...")
            
            # 1. Check Columns
            cursor.execute(f"PRAGMA table_info({table})")
            cols_info = cursor.fetchall()
            cols = [info[1] for info in cols_info]
            
            # Strict Logic Check
            has_roas_comp = 'roascompare_to' in cols
            has_cpa_comp = 'cost_conv_compare_to' in cols
            
            # Use Comparison Logic if available, otherwise fallback to Absolute Thresholds
            # This satisfies user request: "I want everything, even those that can't be compared"
            use_comparison = (has_roas_comp or has_cpa_comp)
            
            print(f"  -> Scanning {table} (Comparison Mode: {use_comparison})")

            all_anomalies_native = []
            
            # 2. Iterate Campaigns
            query = ""
            if 'campaign' in cols:
                query = f"SELECT * FROM {table} WHERE campaign = ?"
            elif 'campaigns' in cols:
                query = f"SELECT * FROM {table} WHERE campaigns = ?"
            else:
                 # Global table (no campaign col), fetch all
                 query = f"SELECT * FROM {table}"
            
            # Helper to execute
            def fetch_rows(camp_name=None):
                params = (camp_name,) if camp_name and '?' in query else ()
                cursor.execute(query, params)
                return cursor.fetchall()

            # Execute Loop
            if '?' in query:
                for camp in campaigns:
                    rows = fetch_rows(camp)
                    process_rows(rows, cols, use_comparison, has_roas_comp, has_cpa_comp, all_anomalies_native)
            else:
                # Run once for global table
                rows = fetch_rows()
                process_rows(rows, cols, use_comparison, has_roas_comp, has_cpa_comp, all_anomalies_native)

            # 3. Create DataFrame for this table
            if all_anomalies_native:
                df = pd.DataFrame(all_anomalies_native)
                sheets_data[legible_name] = df
                print(f"  -> Found {len(all_anomalies_native)} anomalies")
            else:
                print(f"  -> No anomalies found")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

    # Save to Excel
    if sheets_data:
        print(f"Saving to {OUTPUT_FILE}...")
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            for sheet_name, df in sheets_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Export successful!")
    else:
        # Create a dummy file saying no anomalies
        print("No anomalies found in any table.")
        pd.DataFrame({'Info': ['No anomalies found']}).to_excel(OUTPUT_FILE, index=False)


def process_rows(rows, cols, use_comparison, has_roas_comp, has_cpa_comp, result_list):
    for row in rows:
        # Convert row to dict to preserve native format
        row_dict = dict(row)
        
        # Calculate Real-time Logic
        cost = float(row_dict.get('cost') or 0)
        value = float(row_dict.get('conv_value') or 0)
        conv = float(row_dict.get('conversions') or 0)
        
        roas = value / cost if cost > 0 else 0
        cpa = cost / conv if conv > 0 else 0
        
        is_anomaly = False
        
        if use_comparison:
            # 1. Comparison Logic
            # ROAS Drop
            if has_roas_comp:
                roas_comp = float(row_dict.get('roascompare_to') or 0)
                if roas_comp > 0 and roas < (roas_comp * 0.8):
                    is_anomaly = True
                    row_dict['_ISSUE_TYPE'] = 'ROAS Drop'
            
            # CPA Rise
            if has_cpa_comp:
                cpa_comp = float(row_dict.get('cost_conv_compare_to') or 0)
                if cpa_comp > 0 and cpa > (cpa_comp * 1.25):
                    is_anomaly = True
                    if row_dict.get('_ISSUE_TYPE'):
                        row_dict['_ISSUE_TYPE'] += ' & CPA Rise'
                    else:
                        row_dict['_ISSUE_TYPE'] = 'CPA Rise'
        else:
            # 2. Absolute Threshold Logic (For tables without comparison cols like Search Term)
            # Logic: Cost > 20 & ROAS < 1.0 OR Cost > 50 & High CPA
            
            # Low ROAS
            if cost > 20 and roas < 1.0:
                is_anomaly = True
                row_dict['_ISSUE_TYPE'] = 'Low ROAS (Cost > 20)'
            
            # High CPA / Zero Conv
            if cost > 50 and (conv == 0 or cpa > 50):
                is_anomaly = True
                if row_dict.get('_ISSUE_TYPE'):
                    row_dict['_ISSUE_TYPE'] += ' & High CPA/Zero Conv'
                else:
                     row_dict['_ISSUE_TYPE'] = 'High CPA/Zero Conv'

        if is_anomaly:
            result_list.append(row_dict)

if __name__ == "__main__":
    export_anomalies()
