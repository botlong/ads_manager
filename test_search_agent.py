"""
Test script for Search Agent
Tests the call_search_agent function with real campaign data
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.agent_service import call_search_agent, query_db

def test_search_agent():
    print("=" * 60)
    print("TESTING SEARCH AGENT")
    print("=" * 60)
    
    # Find a Search campaign from the database
    campaigns = query_db("""
        SELECT campaign, campaign_type 
        FROM campaign 
        WHERE campaign_type LIKE '%Search%' 
        LIMIT 1
    """)
    
    if not campaigns:
        print("❌ No Search campaigns found in database!")
        return
    
    test_campaign = campaigns[0]['campaign']
    print(f"\n📋 Testing with campaign: {test_campaign}")
    print("-" * 60)
    
    # Simulate issues (as would be detected by scan)
    test_issues = ["ROAS Drop (33.4%)", "CPA Rise (41.1%)"]
    
    # Call the Search Agent
    result = call_search_agent.invoke({
        "campaign_name": test_campaign,
        "issues": test_issues
    })
    
    print("\n" + result)
    print("\n" + "=" * 60)
    print("✅ TEST COMPLETED")
    print("=" * 60)

if __name__ == "__main__":
    test_search_agent()
