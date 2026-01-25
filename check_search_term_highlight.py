
import sqlite3

def check_search_terms():
    conn = sqlite3.connect('d:/ads_manager/ads_data.sqlite')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    campaign_name = "Pmax-5r mini-20250916"
    
    # Check columns first
    cursor.execute("SELECT * FROM search_term LIMIT 1")
    row = cursor.fetchone()
    print(f"Columns: {row.keys()}")

    # Query Top 20 by Cost
    print(f"\nTop 20 Spenders for: {campaign_name}")
    cursor.execute("""
        SELECT search_term, cost, conversions, conv_value_cost 
        FROM search_term 
        WHERE campaign = ? 
        ORDER BY CAST(cost AS REAL) DESC 
        LIMIT 20
    """, (campaign_name,))
    
    rows = cursor.fetchall()
    
    print(f"{'Search Term':<30} | {'Cost':<10} | {'ROAS':<10} | {'Conv':<5} | {'Highlight?'}")
    print("-" * 100)
    
    for row in rows:
        term = row['search_term'] or "NULL"
        cost = float(row['cost'] or 0)
        conv = float(row['conversions'] or 0)
        roas = float(row['conv_value_cost'] or 0)
        
        # Logic
        highlight = False
        reason = ""
        
        # 1. Cost > 20 AND ROAS < 1.0
        if cost > 20 and roas < 1.0:
            highlight = True
            reason = "Low ROAS"
            
        # 2. Cost > 50 AND (Conv == 0) (High CPA check logic simplified here)
        if cost > 50 and conv == 0:
            highlight = True
            reason = "Zero Conv"

        print(f"{term[:30]:<30} | {cost:<10.2f} | {roas:<10.2f} | {conv:<5.0f} | {highlight} {reason}")

    conn.close()

if __name__ == "__main__":
    check_search_terms()
