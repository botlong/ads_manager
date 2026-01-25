
import sqlite3
import sys

def check_data_types():
    with open('d:/ads_manager/debug_output.txt', 'w', encoding='utf-8') as f:
        try:
            conn = sqlite3.connect('d:/ads_manager/ads_data.sqlite')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            campaign_name = "Pmax-5r mini-20250916"
            
            cursor.execute("""
                SELECT search_term, cost, conv_value_cost 
                FROM search_term 
                WHERE campaign = ? 
                AND CAST(cost AS REAL) > 20 
                LIMIT 5
            """, (campaign_name,))
            
            rows = cursor.fetchall()
            
            f.write(f"checking 5 rows for campaign: {campaign_name}\n")
            
            for row in rows:
                cost_raw = row['cost']
                roas_raw = row['conv_value_cost']
                
                f.write(f"Term: {row['search_term']}\n")
                f.write(f"  Cost Raw: {repr(cost_raw)} (Type: {type(cost_raw)})\n")
                f.write(f"  ROAS Raw: {repr(roas_raw)} (Type: {type(roas_raw)})\n")
                
                try:
                    c_float = float(cost_raw)
                    r_float = float(roas_raw)
                    f.write(f"  Parsed: Cost={c_float}, ROAS={r_float}\n")
                    
                    if c_float > 20 and r_float < 1.0:
                         f.write("  -> SHOULD HIGHLIGHT (Cost > 20 and ROAS < 1.0)\n")
                    else:
                         f.write("  -> No Highlight\n")
                         
                except ValueError as e:
                    f.write(f"  Parse Error: {e}\n")
                    
                f.write("-" * 50 + "\n")
                
        except Exception as e:
            f.write(f"Error: {e}\n")
        finally:
            if conn: conn.close()

if __name__ == "__main__":
    check_data_types()
