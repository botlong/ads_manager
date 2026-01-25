
import sqlite3

def check_roas_accuracy():
    try:
        conn = sqlite3.connect('d:/ads_manager/ads_data.sqlite')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print(f"{'Cost':<10} | {'Value':<10} | {'Calc ROAS':<10} | {'Col ROAS':<10} | {'Diff':<10}")
        print("-" * 60)

        # Check Top 20 rows from search_term where cost > 0
        cursor.execute("""
            SELECT cost, conv_value, conv_value_cost 
            FROM search_term 
            WHERE CAST(cost AS REAL) > 0 
            ORDER BY CAST(cost AS REAL) DESC 
            LIMIT 20
        """)
        
        rows = cursor.fetchall()
        
        for row in rows:
            # Handle potential None/Strings
            cost = float(row['cost'] if row['cost'] else 0)
            value = float(row['conv_value'] if row['conv_value'] else 0)
            col_roas = float(row['conv_value_cost'] if row['conv_value_cost'] else 0)
            
            calc_roas = value / cost if cost > 0 else 0
            
            diff = abs(calc_roas - col_roas)
            
            print(f"{cost:<10.2f} | {value:<10.2f} | {calc_roas:<10.2f} | {col_roas:<10.2f} | {diff:<10.4f}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_roas_accuracy()
