import sqlite3
import sys
import os

# Mock the database connection
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), 'ads_data.sqlite')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def test_query(name, sql, params):
    print(f"Testing {name}...", end=" ")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        print("✅ OK")
    except Exception as e:
        print(f"❌ FAIL: {e}")
        print(f"Query: {sql}")
    finally:
        conn.close()

if __name__ == "__main__":
    print("Starting SQL Verification...\n")

    # 1. Scan Query
    test_query(
        "Scan Campaigns", 
        "SELECT campaign, campaign_type, roas, roascompare_to, cost_conv, cost_conv_compare_to, conversions FROM campaign", 
        ()
    )

    # 2. Channel Analysis
    test_query(
        "Channel Analysis",
        "SELECT channels, status, cost, conversions, conv_value, clicks FROM channel WHERE campaigns LIKE ? AND status = 'active' ORDER BY CAST(cost AS REAL) DESC",
        ('%test%',)
    )

    # 3. Product Analysis
    test_query(
        "Product Analysis",
        "SELECT title, item_id, cost, conversions, conv_value_cost FROM product ORDER BY CAST(cost AS REAL) DESC LIMIT 10",
        ()
    )

    # 4. Location Analysis
    test_query(
        "Location Analysis",
        "SELECT matched_location, cost, conversions FROM location_by_cities_all_campaign WHERE campaign = ? AND CAST(cost AS REAL) > 50 AND CAST(conversions AS REAL) = 0 ORDER BY CAST(cost AS REAL) DESC LIMIT 3",
        ('test',)
    )

    # 5. PMax Search Term
    test_query(
        "PMax Search Term",
        "SELECT search_term, cost, conversions FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 20",
        ('test',)
    )

    # 6. Asset Analysis
    test_query(
        "Asset Analysis",
        "SELECT ad_group, campaign, cost, conversions, roas, status FROM asset WHERE ad_group LIKE ? OR campaign LIKE ? ORDER BY CAST(cost AS REAL) DESC LIMIT 15",
        ('%test%', '%test%')
    )

    # 7. Audience (PMax)
    test_query(
        "Audience PMax",
        "SELECT audience_segment, cost, conversions, conv_value_cost FROM audience WHERE campaign LIKE ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10",
        ('%test%',)
    )

    # 8. Age
    test_query(
        "Age Demographics",
        "SELECT age, cost, conv_value_cost FROM age WHERE campaign LIKE ? AND CAST(cost AS REAL) > 50 ORDER BY CAST(cost AS REAL) DESC LIMIT 5",
        ('%test%',)
    )

    # 9. Gender
    test_query(
        "Gender Demographics",
        "SELECT gender, cost, conv_value_cost FROM gender WHERE campaign LIKE ? ORDER BY CAST(cost AS REAL) DESC",
        ('%test%',)
    )

    # 10. Ad Schedule (PMax)
    test_query(
        "Ad Schedule PMax",
        "SELECT day_and_time, cost, conversions, conv_value_cost FROM ad_schedule WHERE campaign LIKE ? AND CAST(cost AS REAL) > 20 ORDER BY CAST(cost AS REAL) DESC LIMIT 10",
        ('%test%',)
    )

    # 11. Search Agents - Search Term
    test_query(
        "Search Agent - Terms",
        "SELECT search_term, match_type, cost, conversions, clicks, impr, cost_conv as cpa, conv_value_cost, conv_value FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 20",
        ('test',)
    )

    # 12. Match Type Stats
    test_query(
        "Match Type Stats",
        "SELECT match_type, SUM(CAST(cost AS REAL)) as total_cost, SUM(CAST(conversions AS REAL)) as total_conv, SUM(CAST(conv_value AS REAL)) as total_value, COUNT(*) as term_count FROM search_term WHERE campaign = ? GROUP BY match_type ORDER BY total_cost DESC",
        ('test',)
    )

    # 13. Search Agent - Audience
    test_query(
        "Search Agent - Audience",
        "SELECT audience_segment, cost, conversions, conv_value_cost, cost_conv as cpa FROM audience WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10",
        ('test',)
    )

    # 14. Search Agent - Ad Schedule
    test_query(
        "Search Agent - Ad Schedule",
        "SELECT day_and_time, cost, conversions, conv_value_cost, cost_conv as cpa FROM ad_schedule WHERE campaign = ? AND CAST(cost AS REAL) > 20 ORDER BY CAST(cost AS REAL) DESC LIMIT 10",
        ('test',)
    )

    # 15. Search Agent - Age
    test_query(
        "Search Agent - Age",
        "SELECT age, cost, conversions, conv_value_cost FROM age WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 5",
        ('test',)
    )

    # 16. Search Agent - Gender
    test_query(
        "Search Agent - Gender",
        "SELECT gender, cost, conversions, conv_value_cost FROM gender WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC",
        ('test',)
    )
