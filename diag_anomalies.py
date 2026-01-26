import sys
import os
# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from agent_service import get_campaign_anomalies_logic, scan_campaigns_for_anomalies
from expert_system import ExpertEngine

def test_diag():
    target_date = "2026-01-18"
    print(f"--- Testing get_campaign_anomalies_logic for {target_date} ---")
    anomalies = get_campaign_anomalies_logic(target_date)
    print(f"Found {len(anomalies)} anomalies:")
    for a in anomalies:
        print(f" - {a['campaign']}: {a['reason']}")

    print("\n--- Testing scan_campaigns_for_anomalies full report ---")
    report = scan_campaigns_for_anomalies.invoke({"target_date": target_date})
    print("Report Content Preview (First 500 chars):")
    print(report[:500] + "...")
    
    # Count headers #### ⚠️
    headers_count = report.count("#### ⚠️")
    print(f"\nTotal Headers found in report: {headers_count}")

if __name__ == "__main__":
    test_diag()
