import sqlite3
import pandas as pd
import sys

# Set output encoding
sys.stdout = reconfigure_stdout = open('master_agent_output.txt', 'w', encoding='utf-8')

DB_FILE = 'ads_data.sqlite'

class MasterAgent:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.anomalies = []

    def load_data(self):
        # Load necessary columns
        # sanitize names based on logic:
        # ROAS -> roas
        # ROAS(Compare to) -> roas_compare_to
        # Cost / conv. -> cost_conv
        # Cost / conv. (Compare to) -> cost_conv_compare_to
        # Conversions -> conversions
        # Campaign -> campaign
        # Campaign type -> campaign_type
        
        query = """
            SELECT 
                campaign, 
                campaign_type, 
                roas, 
                roascompare_to, 
                cost_conv, 
                cost_conv_compare_to, 
                conversions
            FROM campaign
        """
        try:
            self.df = pd.read_sql(query, self.conn)
        except Exception as e:
            print(f"Error loading data: {e}")
            # Fallback to fetching all columns to debug if needed, but for now just exit
            sys.exit(1)

    def scan_phase_1(self):
        print("Phase 1: 快速扫描与报警 (Default Mode)")
        print("-" * 50)
        
        found_issues = False
        
        for index, row in self.df.iterrows():
            # Filter condition: Conversions < 3
            if row['conversions'] < 3:
                continue
            
            # Filter summary rows
            if not row['campaign'] or row['campaign'] in ['--', 'Total', 'None']:
                continue
            
            issues = []
            
            # 1. ROAS Alarm: ROAS < ROAS(Compare to) * 0.8
            # Handle potential None/Strings
            try:
                roas = float(row['roas']) if row['roas'] else 0
                roas_comp = float(row['roascompare_to']) if row['roascompare_to'] else 0
                
                if roas_comp > 0 and roas < (roas_comp * 0.8):
                    decline_pct = ((roas_comp - roas) / roas_comp) * 100
                    issues.append(f"ROAS: {roas:.2f} vs {roas_comp:.2f} (下降 {decline_pct:.1f}%) 🔴")
            except ValueError:
                pass # Skip if data is bad

            # 2. CPA Alarm: Cost/conv > Cost/conv(Compare to) * 1.25
            try:
                cpa = float(row['cost_conv']) if row['cost_conv'] else 0
                cpa_comp = float(row['cost_conv_compare_to']) if row['cost_conv_compare_to'] else 0
                
                if cpa_comp > 0 and cpa > (cpa_comp * 1.25):
                    increase_pct = ((cpa - cpa_comp) / cpa_comp) * 100
                    issues.append(f"CPA: {cpa:.2f} vs {cpa_comp:.2f} (上升 {increase_pct:.1f}%)")
            except ValueError:
                pass

            if issues:
                found_issues = True
                self.anomalies.append({
                    'campaign': row['campaign'],
                    'type': row['campaign_type'],
                    'issues': issues
                })

        if not found_issues:
            print("✅ 本周数据扫描完成，所有 Campaign 的 ROAS 和 CPA 波动均在正常范围内。")
        else:
            print("⚠️ **监测报告：发现异常 Campaign**")
            for i, Anomaly in enumerate(self.anomalies, 1):
                print(f"**{i}. {Anomaly['campaign']}** (`{Anomaly['type']}`)")
                for issue in Anomaly['issues']:
                    print(f"   - {issue}")
            print("\n(等待用户指令...)")

    def diagnose_phase_2(self):
        # Simulate user asking for diagnosis for all anomalies
        print("\nPhase 2: Depth Diagnosis (Simulated Trigger)")
        print("-" * 50)
        
        if not self.anomalies:
            print("No anomalies to diagnose.")
            return

        for Anomaly in self.anomalies:
            c_type = Anomaly['type']
            c_name = Anomaly['campaign']
            
            print(f"\n[Analysing {c_name} ({c_type})]")
            
            if "Performance Max" in str(c_type):
                print(f">> 呼叫 `PMax_Analysis_Agent`")
                print(f"   Context: 提取 Asset, Product, Channel, Location")
                print(f"   Instruction: \"该 PMax 广告系列 ROAS 下降/CPA 上升。请分析 Asset 评级、低毛利商品消耗以及 Display/Video 渠道的预算占比情况。\"")
                
            elif "Search" in str(c_type):
                print(f">> 呼叫 `Search_Analysis_Agent`")
                print(f"   Context: 提取 Search Term, Audience, Age, Gender, Ad Schedule")
                print(f"   Instruction: \"该搜索广告系列效率下降。请重点排查广泛匹配(Broad Match)带来的无效搜索词，以及特定受众群体的 CPA 飙升情况。\"")
            
            else:
                print(f">> 未知类型，建议人工检查。")

    def run(self):
        self.load_data()
        self.scan_phase_1()
        # Uncomment to simulate phase 2 immediately
        # self.diagnose_phase_2()

if __name__ == "__main__":
    agent = MasterAgent(DB_FILE)
    agent.run()
