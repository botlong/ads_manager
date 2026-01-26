import os
import sqlite3
import pandas as pd
from typing import List, Dict, Any, TypedDict, Annotated
import operator
import json
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode

# Load env vars
load_dotenv()

# Path to DB
DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ads_data.sqlite')

# Configuration
MAIN_MODEL_NAME = os.getenv("MAIN_MODEL_NAME")
SUB_MODEL_NAME = os.getenv("SUB_MAIN_MODEL_NAM")
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")
MAX_CONTEXT_CHARACTERS = int(os.getenv("MAX_CONTEXT_CHARACTERS", 30000))

# Initialize LLMs Globally
main_llm = ChatOpenAI(
    model=MAIN_MODEL_NAME,
    base_url=BASE_URL,
    api_key=API_KEY,
    temperature=0,
    streaming=True
)

# --- Table Expert Knowledge ---
# This dict provides specific metric descriptions and diagnostic focus for the sub-agent
TABLE_EXPERT_KNOWLEDGE = {
    "age": {
        "title": "Age Demographics Expert (年龄分层专家)",
        "focus": "Audit demographic efficiency with high statistical stability.",
        "metrics_desc": "age: Range, bid_adj: Existing Modifier, cost: Spend, conversions: Units, ctr: CTR",
        "expert_rules": """
        - **Tier 1: Observation (Low Sample)**: If 'clicks' < 25, status is "Too early to optimize". Only note extreme CTR anomalies.
        - **Tier 2: Risk (Delayed Conv Guard)**: If segment is < 7 days old, do not recommend exclusion. Only minor bid reduction if CPA > 2x avg.
        - **Tier 3: Actionable (High Confidence)**: Spend > 2x Account CPA AND 0 Conv over 14+ days -> Recommend -50% bid or exclusion.
        - **Unknown Guard**: Protected status. Do not exclude unless spend is 3x higher than converted segments with 0 ROAS.
        """
    },
    "gender": {
        "title": "Gender Demographics Expert (性别分层专家)",
        "focus": "Identify structural gender imbalances.",
        "metrics_desc": "gender: Category, cost: Spend, conversions: Units, ctr: CTR",
        "expert_rules": """
        - **Confidence**: Requires min 100 clicks or 10 conversions for major advice.
        - **Protection**: If one gender has high CTR but 0 Conv, check if Landing Page is gender-neutral before excluding.
        """
    },
    "search_term": {
        "title": "Search Term Analyst (搜索词专家)",
        "focus": "Aggressive junk filtering with Brand Protection.",
        "metrics_desc": "search_term: query, match_type: Match, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Brand Umbrella**: If search_term contains Brand/Product core name, mark as "Strategic Asset". Keep even if 0 ROAS for now.
        - **Junk Patterns**: Immediate 'Critical' for terms like 'free', 'repair', 'whatsapp', 'support', 'login' (Non-sales intent).
        - **Broad Match Audit**: If > 60% of waste flows through 'Broad' match, recommend shifting to Phrase/Exact.
        - **Tiering**: Spend > 1.5x CPA + 0 Conv -> 'High Confidence' Negative Recommendation.
        """
    },
    "location_by_cities_all_campaign": {
        "title": "Geography Analyst (地域/城市专家)",
        "focus": "Three-tier regional auditing.",
        "metrics_desc": "matched_location: Location, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Tier 1: High Confidence Blackhole**: Spend >= $100 AND 0 Conv AND Historical 30d Conv = 0 -> Recommend Exclusion.
        - **Tier 2: Efficiency Risk**: CPA >= 2x Avg CPA -> Recommend -30% bid reduction.
        - **Tier 3: Observation**: Spend < $50 OR Clicks < 50 -> Status "Observing". Data too sparse for regional exclusion.
        """
    },
    "ad_schedule": {
        "title": "Time & Schedule Analyst (分时专家)",
        "focus": "Peak/Trough pattern identification with stability guard.",
        "metrics_desc": "day_and_time: Slot, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Stability Rule**: Minimum 100 clicks per slot (over 30 days) required for -50% modifier recommendation.
        - **Delayed Return Protection**: Be cautious with 00:00-05:00 slots as conversions often attribute late. 
        - **Action**: Only target extreme 'Midnight Waste' (Spend > 3x CPA, 0 Conv) for aggressive exclusion.
        """
    },
    "audience": {
        "title": "Audience Segment Analyst (受众专家)",
        "focus": "Signal-to-noise auditing.",
        "metrics_desc": "audience_segment: Signal, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Tiering**: Focus on high-spend zero-ROI 'In-market' segments.
        - **Confidence**: Require min 5 Conversions before recommending 'Targeting' instead of 'Observation'.
        """
    },
    "product": {
        "title": "Product SKU Analyst (产品/货架专家)",
        "focus": "Zombie detection and Cold Start Protection.",
        "metrics_desc": "title: Name, item_id: SKU, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Cold Start Protection**: New SKUs (Total Spend < $30) are 'Protected'. Do not flag as Zombie yet.
        - **High-Confidence Zombie**: Spend > $80 AND 0 Conv -> Recommend Status 'Excluded' in Listing Group.
        - **Budget Hegemony**: If 1 product takes > 85% budget, flag as "Testing Starvation Risk".
        """
    },
    "channel": {
        "title": "PMax Channel Analyst (PMax 渠道专家)",
        "focus": "Cross-channel subsidy auditing.",
        "metrics_desc": "channels: Type, cost: Spend, conversions: Conv, roas: ROAS",
        "expert_rules": """
        - **Subsidy Check**: Flag if 'Shopping' (Feed) is subsidizing > 40% waste in 'Video/Display'.
        - **Structure Risk**: If Video Spend > 30% AND Video CPA > 2.5x Target -> High Risk Recommendation.
        """
    }
}

sub_llm = ChatOpenAI(
    model=SUB_MODEL_NAME,
    base_url=BASE_URL,
    api_key=API_KEY,
    temperature=0.1, # Slightly creative for reporting
    streaming=False  # Sub-agent usually returns full report
)

# --- Database Helpers ---

def get_db_connection():
    return sqlite3.connect(DB_FILE)

def query_db(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"DB Error: {e}")
        print(f"Failed Query: {query[:200]}...")  # Show first 200 chars
        print(f"Params: {params}")
        return []
    finally:
        conn.close()

def query_value(query: str, params: tuple = ()):
    """Helper to get a single value from DB"""
    res = query_db(query, params)
    if res:
        return list(res[0].values())[0]
    return 0

def safe_truncate_data(data_list: List[Dict], max_chars: int) -> str:
    """
    Incrementally adds rows to the JSON output until max_chars is reached.
    Ensures we don't break the model context while keeping the most important (top-ranked) data.
    """
    if not data_list:
        return "[]"
    
    truncated_list = []
    current_size = 0
    # Reserve room for metadata and surrounding JSON markers
    available_chars = max_chars - 500 
    
    for row in data_list:
        row_json = json.dumps(row, ensure_ascii=False)
        if current_size + len(row_json) + 2 > available_chars:
            break
        truncated_list.append(row)
        current_size += len(row_json) + 2
        
    result = {
        "data": truncated_list,
        "metadata": {
            "total_rows_queried": len(data_list),
            "rows_included": len(truncated_list),
            "is_truncated": len(truncated_list) < len(data_list),
            "truncation_warning": "DATA TRUNCATED DUE TO CONTEXT LIMIT" if len(truncated_list) < len(data_list) else "None"
        }
    }
    return json.dumps(result, ensure_ascii=False, indent=2)

# --- Tools Definition ---

def analyze_specific_table(campaign_name: str, table_name: str, start_date: str = None, end_date: str = None) -> str:
    """
    Calls a specialized sub-agent to analyze a specific table for a campaign within a date range.
    Input: campaign_name (exact name), table_name (e.g., 'age', 'search_term'), start_date (YYYY-MM-DD), end_date (YYYY-MM-DD).
    """
    # Strict validation: Only allow analysis if it matches expert knowledge
    if table_name not in TABLE_EXPERT_KNOWLEDGE:
        return f"Error: No expert knowledge defined for table '{table_name}'. You cannot analyze this table yet."

    # 1. Fetch Main Campaign Context (The "Big Picture")
    main_stats = query_db("SELECT cost, conversions, roas, cpa FROM campaign WHERE campaign = ?", (campaign_name,))
    context_str = "No main campaign aggregate found."
    if main_stats:
        s = main_stats[0]
        context_str = f"Main Campaign Avg: Cost ${s.get('cost')}, ROAS {s.get('roas')}, CPA ${s.get('cpa')}, Conv {s.get('conversions')}"

    campaign_col = 'campaign'
    if table_name == 'channel': campaign_col = 'campaigns'

    # 2. Fetch Targeted Table Data with Date Filter
    where_conditions = [f"{campaign_col} = ?"]
    params = [campaign_name]
    
    if start_date:
        where_conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        where_conditions.append("date <= ?")
        params.append(end_date)
        
    where_clause = " AND ".join(where_conditions)
    query = f"SELECT * FROM {table_name} WHERE {where_clause} ORDER BY CAST(cost AS REAL) DESC LIMIT 15"
    table_data = query_db(query, tuple(params))
    
    if not table_data:
        return f"Found no data in '{table_name}' for campaign '{campaign_name}'."

    # 3. Invoke Sub-Agent with Expertise
    expert = TABLE_EXPERT_KNOWLEDGE[table_name]
    
    prompt = f"""
    You are a specialized Data Analyst expert in Google Ads, focusing on: {expert['title']}.
    
    **Analysis Goal**: {expert['focus']}
    **Metrics Guide**: {expert['metrics_desc']}
    
    **Expert Analysis Logic (Strictly follow these tiers)**:
    {expert.get('expert_rules', 'Analyze for efficiency and anomalies.')}
    
    ---
    **Campaign Context**: {campaign_name}
    **Analysis Period**: {start_date or 'ALL'} to {end_date or 'ALL'}
    **Aggregate Benchmark**: {context_str}
    
    **Raw Data (Sorted by significance):**
    {safe_truncate_data(table_data, MAX_CONTEXT_CHARACTERS)}
    
    ---
    **Output Requirements**:
    1. **Data Coverage**: Start with "Data Coverage Index: X% of campaign cost analyzed".
    2. **Status & Confidence**: Clearly state "Status: [Too early to optimize | Actionable]" and "Confidence: [High|Medium|Low]".
    3. **Actionable Actions**: Audit existing bid modifiers. Suggest changes ONLY if Tier 3 (Actionable) thresholds are met.
    4. **Logic**: Briefly explain which Tier the data falls into.
    5. Output in concise Markdown (Chinese).
    """

    res = sub_llm.invoke(prompt)
    return res.content

@tool
def scan_campaigns_for_anomalies() -> str:
    """
    Scans the 'campaign' table to identify statistically significant anomalies.
    Rules:
    1. ROAS Drop: Current ROAS < Previous 7d Avg * 0.7 (30% drop)
    2. CPA Rise: Current CPA > Previous 7d Avg * 1.3 (30% rise)
    3. Stability: Cost > $20 (Ignore noise in tiny budgets)
    """
    query = """
        SELECT campaign, campaign_type, cost, roas, roas_before_7d_average, cpa, cpa_before_7d_average, conversions
        FROM campaign
    """
    rows = query_db(query)
    anomalies = []
    
    for row in rows:
        try:
            conv = float(row['conversions']) if row['conversions'] else 0
            cost = float(row['cost']) if row['cost'] else 0
            roas = float(row['roas']) if row['roas'] else 0
            roas_avg = float(row['roas_before_7d_average']) if row['roas_before_7d_average'] else 0
            cpa = float(row['cpa']) if row['cpa'] else 0
            cpa_avg = float(row['cpa_before_7d_average']) if row['cpa_before_7d_average'] else 0
        except: continue

        c_name = row['campaign']
        if not c_name or c_name in ['--', 'Total', 'None']: continue

        issues = []
        
        # Stability check & ROAS Drop
        if roas_avg > 0 and roas < (roas_avg * 0.7) and cost > 20:
             issues.append(f"Significant ROAS Drop (Current {roas:.2f} vs Avg {roas_avg:.2f})")
        
        # CPA Rise
        if cpa_avg > 0 and cpa > (cpa_avg * 1.3) and cost > 20:
             issues.append(f"Significant CPA Rise (Current {cpa:.2f} vs Avg {cpa_avg:.2f})")

        if issues:
            anomalies.append({
                "campaign_name": c_name,
                "issues": issues,
                "confidence_signal": "High" if conv >= 3 else "Medium"
            })

    if not anomalies:
        return "No significant anomalies detected."
    
    return json.dumps(anomalies, indent=2)

@tool
def call_pmax_agent(campaign_name: str, issues: List[str], start_date: str = None, end_date: str = None) -> str:
    """
    Calls the PMax Sub-Agent to analyze a specific Performance Max campaign within a date range.
    Performs deep dive into Channels, Products, Locations, and Search Terms.
    """
    report = [f"### 🕵️ PMax Deep Dive: {campaign_name}"]
    report.append(f"**Trigger Issues**: {', '.join(issues)}\n")

    # A. Channel Analysis (Calculated Metrics)
    try:
        # Fetch raw metrics with date filter
        where_conditions = ["campaigns LIKE ?", "status = 'active'"]
        params = [f"%{campaign_name}%"]
        if start_date:
            where_conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_conditions.append("date <= ?")
            params.append(end_date)

        where_clause = " AND ".join(where_conditions)
        channels_data = query_db(
            f"""
            SELECT channels, status, cost, conversions, conv_value, clicks
            FROM channel 
            WHERE {where_clause}
            ORDER BY CAST(cost AS REAL) DESC
            """, 
            tuple(params)
        )
        
        if channels_data:
            report.append("#### 📡 A. Channel Analysis (数据来源: channel 表)")
            anomalies_found = False
            
            for c in channels_data:
                channel_name = c.get('channels', 'Unknown')
                
                # Safe float conversion
                cost = float(c.get('cost', 0) or 0)
                conv = float(c.get('conversions', 0) or 0)
                clicks = float(c.get('clicks', 0) or 0)
                val = float(c.get('conv_value', 0) or 0)
                
                # Calculate Metrics
                roas = (val / cost) if cost > 0 else 0.0
                cpa = (cost / conv) if conv > 0 else 0.0
                cvr = (conv / clicks * 100) if clicks > 0 else 0.0
                aov = (val / conv) if conv > 0 else 0.0
                
                # Anomaly Detection (Thresholds)
                channel_issues = []
                if roas < 1.0 and cost > 10: channel_issues.append(f"Low ROAS ({roas:.2f})")
                if cpa > 50 and conv > 0: channel_issues.append(f"High CPA ({cpa:.2f})")
                if cvr < 1.0 and clicks > 50: channel_issues.append(f"Low CVR ({cvr:.1f}%)")
                
                # Format output
                metrics_str = f"Cost: ${cost:.2f} | ROAS: {roas:.2f} | CPA: ${cpa:.2f} | CVR: {cvr:.1f}% | AOV: ${aov:.2f}"
                
                if channel_issues:
                    anomalies_found = True
                    report.append(f"- ⚠️ **{channel_name}**: {', '.join(channel_issues)}")
                    report.append(f"  ({metrics_str})")
                else:
                    report.append(f"- ✅ **{channel_name}**: Performing normally.")
                    report.append(f"  ({metrics_str})")
            
            if not anomalies_found:
                report.append("👉 No specific channel anomalies detected among active channels.")
            report.append("")
        else:
            report.append("#### 📡 A. Channel Analysis")
            report.append("ℹ️ No active channel data found for this campaign.")
            report.append("")

    except Exception as e:
        report.append(f"Error in Channel Analysis: {e}")

    # B. Product Analysis (The "Shelf")
    # Logic: Zombie (Cost > 50, Conv=0), Inefficient (ROAS < 0.5)
    try:
        products = query_db("SELECT title, item_id, cost, conversions, conv_value_cost FROM product ORDER BY CAST(cost AS REAL) DESC LIMIT 10")
        zombies = []
        inefficient = []
        
        for p in products:
            cost = float(p.get('cost', 0))
            conv = float(p.get('conversions', 0))
            roas = float(p.get('conv_value_cost', 0)) if p.get('conv_value_cost') else 0
            item_id = p.get('item_id', 'N/A')
            title = p.get('title', 'Unknown')
            
            if cost > 50 and conv == 0:
                zombies.append(f"{title} (ID: {item_id}) - Cost ${cost:.2f}, 0 Conv")
            elif cost > 20 and roas < 0.5:
                inefficient.append(f"{title} (ID: {item_id}) - ROAS {roas:.2f}, Cost ${cost:.2f}")

        report.append("#### 📦 B. Product Analysis (数据来源: product 表 - Account Level)")
        product_json = safe_truncate_data(products, MAX_CONTEXT_CHARACTERS // 4) # Split quota among sections
        report.append(f"```json\n{product_json}\n```")
        
        if zombies:
            report.append("❌ **Zombie Products (> $50, 0 Conv)**:")
            for z in zombies: report.append(f"  - {z}")
        if inefficient:
            report.append("⚠️ **Inefficient Products (ROAS < 0.5)**:")
            for i in inefficient: report.append(f"  - {i}")
    except Exception as e:
        report.append(f"Error in Product Analysis: {e}")

    # C. Location Analysis
    # Logic: Cost > 50, Conv=0
    try:
        locs = query_db("SELECT matched_location, cost, conversions FROM location_by_cities_all_campaign WHERE campaign = ? AND CAST(cost AS REAL) > 50 AND CAST(conversions AS REAL) = 0 ORDER BY CAST(cost AS REAL) DESC LIMIT 3", (campaign_name,))
        report.append("#### 🌍 C. Location Analysis (数据来源: location_by_cities_... 表)")
        if locs:
            report.append("❌ **Money Wasting Locations**:")
            for l in locs:
                report.append(f"- **{l.get('matched_location')}**: Cost ${l.get('cost')}, 0 Conv")
            report.append("👉 **Action**: Exclude these locations in Campaign Settings.")
        else:
            report.append("✅ No high-spend zero-conversion locations found.")
        report.append("")
    except Exception as e:
        report.append(f"Error in Location Analysis: {e}")

    # D. Search Term Analysis (PMax Search Terms)
    # Logic: Look for bad patterns (free, repair, etc.)
    try:
        bad_keywords = ['free', 'repair', 'login', 'support', 'manual', 'review']
        terms = query_db("SELECT search_term, cost, conversions FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 20", (campaign_name,))
        
        found_bad_terms = []
        for t in terms:
            term = t.get('search_term', '').lower()
            cost = float(t.get('cost', 0))
            if any(bk in term for bk in bad_keywords):
                found_bad_terms.append(f"'{term}' (Cost ${cost})")
        
        report.append("#### 🔍 D. Search Term Analysis (数据来源: search_term 表)")
        if found_bad_terms:
            report.append("⚠️ **Irrelevant Search Terms Detected**:")
            for ft in found_bad_terms: report.append(f"- {ft}")
            report.append("👉 **Action**: Add these as Account-Level Negative Keywords.")
        else:
            report.append("✅ No obvious junk keywords found in top spenders.")
            
    except Exception as e:
        report.append(f"Error in Search Term Analysis: {e}")

    # E. Asset Analysis
    try:
        assets = query_db("""
            SELECT ad_group, campaign, cost, conversions, roas, status
            FROM asset
            WHERE ad_group LIKE ? OR campaign LIKE ?
            ORDER BY CAST(cost AS REAL) DESC
            LIMIT 15
        """, (f"%{campaign_name}%", f"%{campaign_name}%"))
        
        report.append("#### 🎨 E. Asset Analysis (数据来源: asset 表)")
        
        if assets:
            # Use ROAS to identify poor performers since 'performance' field doesn't exist
            poor_assets = [f"{a.get('ad_group', 'Unknown')}: ROAS {float(a.get('roas', 0) or 0):.2f}, ${float(a.get('cost', 0) or 0):.2f}"
                          for a in assets if float(a.get('roas', 0) or 0) < 1.5 and float(a.get('cost', 0) or 0) > 30][:5]
            
            if poor_assets:
                report.append("❌ **Low-Performing Assets (ROAS < 1.5)**:")
                for pa in poor_assets: report.append(f"  - {pa}")
                report.append("👉 **Action**: Review and optimize underperforming ad groups.")
            else:
                report.append("✅ No major asset performance issues.")
            report.append("")
        else:
            report.append("ℹ️ No asset data available.")
            report.append("")
    except Exception as e:
        report.append(f"Error in Asset Analysis: {e}")
        report.append("")

    # F. Audience Signal Analysis
    try:
        audiences = query_db("""
            SELECT audience_segment, cost, conversions, conv_value_cost
            FROM audience WHERE campaign LIKE ?
            ORDER BY CAST(cost AS REAL) DESC LIMIT 10
        """, (f"%{campaign_name}%",))
        
        report.append("#### 👥 F. Audience Signal Analysis (数据来源: audience 表)")
        
        if audiences:
            weak = [f"{a.get('audience_segment')} - ${float(a.get('cost', 0) or 0):.2f}, ROAS {float(a.get('conv_value_cost', 0) or 0):.2f}"
                   for a in audiences if float(a.get('cost', 0) or 0) > 50 and float(a.get('conv_value_cost', 0) or 0) < 2.0][:3]
            
            if weak:
                report.append("⚠️ **Weak Audience Signals**:")
                for w in weak: report.append(f"  - {w}")
                report.append("👉 **Action**: Consider removing these signals.")
            else:
                report.append("✅ Audience signals performing adequately.")
            report.append("")
        else:
            report.append("ℹ️ No audience signal data.")
            report.append("")
    except Exception as e:
        report.append(f"Error in Audience Analysis: {e}")
        report.append("")

    # G. Demographics (Age + Gender)
    try:
        age_data = query_db("SELECT age, cost, conv_value_cost FROM age WHERE campaign LIKE ? AND CAST(cost AS REAL) > 50 ORDER BY CAST(cost AS REAL) DESC LIMIT 5", (f"%{campaign_name}%",))
        gender_data = query_db("SELECT gender, cost, conv_value_cost FROM gender WHERE campaign LIKE ? ORDER BY CAST(cost AS REAL) DESC", (f"%{campaign_name}%",))
        
        report.append("#### 🎯 G. Demographics (数据来源: age, gender 表)")
        
        demo_issues = []
        for age in age_data:
            if float(age.get('conv_value_cost', 0) or 0) < 1.5:
                demo_issues.append(f"Age {age.get('age')}: ${float(age.get('cost', 0) or 0):.0f}, ROAS {float(age.get('conv_value_cost', 0) or 0):.2f}")
        for gen in gender_data:
            if float(gen.get('cost', 0) or 0) > 100 and float(gen.get('conv_value_cost', 0) or 0) < 2.0:
                demo_issues.append(f"Gender {gen.get('gender')}: ${float(gen.get('cost', 0) or 0):.0f}, ROAS {float(gen.get('conv_value_cost', 0) or 0):.2f}")
        
        if demo_issues:
            report.append("⚠️ **Underperforming Demographics**:")
            for di in demo_issues[:4]: report.append(f"  - {di}")
            report.append("👉 **Action**: Consider bid adjustments.")
        else:
            report.append("✅ Demographics look balanced.")
        report.append("")
    except Exception as e:
        report.append(f"Error in Demographics: {e}")
        report.append("")

    # H. Ad Schedule
    try:
        schedules = query_db("SELECT day_and_time, cost, conversions, conv_value_cost FROM ad_schedule WHERE campaign LIKE ? AND CAST(cost AS REAL) > 20 ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (f"%{campaign_name}%",))
        
        report.append("#### ⏰ H. Ad Schedule Analysis (数据来源: ad_schedule 表)")
        
        if schedules:
            # Parse day_and_time field (e.g., "Monday 14:00")
            bad_slots = []
            for s in schedules:
                if float(s.get('cost', 0) or 0) > 40 and float(s.get('conv_value_cost', 0) or 0) < 1.5:
                    day_time = s.get('day_and_time', 'Unknown')
                    cost = float(s.get('cost', 0) or 0)
                    roas = float(s.get('conv_value_cost', 0) or 0)
                    bad_slots.append(f"{day_time} - ${cost:.2f}, ROAS {roas:.2f}")
            
            if bad_slots[:5]:
                report.append("⚠️ **Inefficient Time Slots**:")
                for bs in bad_slots[:5]: report.append(f"  - {bs}")
                report.append("👉 **Action**: Apply bid adjustments (-30% to -50%).")
            else:
                report.append("✅ No major time-of-day issues.")
            report.append("")
        else:
            report.append("ℹ️ Insufficient ad schedule data.")
            report.append("")
    except Exception as e:
        report.append(f"Error in Ad Schedule: {e}")
        report.append("")

    return "\n".join(report)

@tool
def call_search_agent(campaign_name: str, issues: List[str], start_date: str = None, end_date: str = None) -> str:
    """
    Calls the Search Sub-Agent to analyze a specific Search campaign within a date range.
    Uses the flash model to deeper analyze Search Terms, Match Types, Audiences.
    """
    data_context = []
    data_context.append(f"Campaign: {campaign_name}")
    data_context.append(f"Trigger Issues: {', '.join(issues)}")

    # A. Search Terms
    try:
        where_conditions = ["campaign = ?"]
        params = [campaign_name]
        if start_date:
            where_conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_conditions.append("date <= ?")
            params.append(end_date)
        
        where_clause = " AND ".join(where_conditions)
        terms = query_db(f"SELECT search_term, match_type, cost, conversions, conv_value_cost FROM search_term WHERE {where_clause} ORDER BY CAST(cost AS REAL) DESC LIMIT 20", tuple(params))
        data_context.append(f"\n[Search Terms (Top 20 Impact)]: {json.dumps([dict(r) for r in terms], ensure_ascii=False)}")
    except: pass

    # B. Match Types
    try:
        match_stats = query_db("""
            SELECT match_type, SUM(CAST(cost AS REAL)) as total_cost, SUM(CAST(conversions AS REAL)) as total_conv, SUM(CAST(conv_value AS REAL)) as total_value
            FROM search_term WHERE campaign = ? GROUP BY match_type ORDER BY total_cost DESC
        """, (campaign_name,))
        data_context.append(f"\n[Match Type Stats]: {json.dumps([dict(r) for r in match_stats], ensure_ascii=False)}")
    except: pass

    # C. Audiences
    try:
        audiences = query_db("SELECT audience_segment, cost, conversions, conv_value_cost FROM audience WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        data_context.append(f"\n[Audience Data]: {json.dumps([dict(r) for r in audiences], ensure_ascii=False)}")
    except: pass

    # Invoke Sub-Agent LLM
    prompt = f"""
    You are a specialized Search Ads Analysis Agent. Analyze the provided data for Search Campaign '{campaign_name}' and produce a concise report in Chinese.

    **Data Context:**
    {chr(10).join(data_context)}

    **Analysis Requirements:**
    1. **Search Terms**: Identify irrelevant junk terms (negative opportunities) and high-cost/zero-conv items.
    2. **Match Types**: Compare Broad vs Phrase/Exact performance. Recommend tightening if Broad is inefficient.
    3. **Actionable Advice**: For every issue, recommend a specific action (e.g. "Add negative: 'free'").

    **Output Format (Markdown):**
    ### 🔍 Search Campaign Deep Dive: {campaign_name}
    #### 1. 核心发现
    (Summary)
    #### 2. 详细分析
    - **搜索词**: ...
    - **匹配类型**: ...
    #### 3. 优化建议 (Actions)
    - [ ] Action 1
    - [ ] Action 2
    """

    msg = sub_llm.invoke(prompt)
    return msg.content


# --- LangGraph Setup ---

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    selected_tables: List[str]

class AgentService:
    def __init__(self):
        print(f"Initializing Main Agent with model={MAIN_MODEL_NAME}")
        self.llm = main_llm
        
        self.tools = [scan_campaigns_for_anomalies, analyze_specific_table, call_pmax_agent, call_search_agent]
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        
        self._init_prefs_db()

        workflow = StateGraph(AgentState)
        workflow.add_node("agent", self.call_model)
        workflow.add_node("tools", ToolNode(self.tools))
        workflow.set_entry_point("agent")
        workflow.add_conditional_edges("agent", self.should_continue, {"continue": "tools", "end": END})
        workflow.add_edge("tools", "agent")
        self.app = workflow.compile()

    def _init_prefs_db(self):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                table_name TEXT,
                item_identifier TEXT,
                is_pinned INTEGER DEFAULT 0,
                display_order INTEGER DEFAULT 0,
                PRIMARY KEY (table_name, item_identifier)
            )
        """)
        conn.commit()
        conn.close()

    def call_model(self, state: AgentState):
        messages = state['messages']
        selected = state.get('selected_tables', [])
        
        # Build dynamic expertise list for the prompt
        available_experts = []
        for table_id in selected:
            if table_id in TABLE_EXPERT_KNOWLEDGE:
                info = TABLE_EXPERT_KNOWLEDGE[table_id]
                available_experts.append(f"- 【{info['title']}专家】: 专注于 {info['focus']}。使用工具时传入 table_name='{table_id}'")

        expertise_section = "\n".join(available_experts) if available_experts else "当前未开启任何专项深度诊断 (用户仅关注汇总数据)。"

        if not isinstance(messages[0], SystemMessage):
            system_prompt = SystemMessage(content=f"""你是 AdsManager Main Agent (任务调度器)。
你的职责是实时监控广告表现，并协调“专项专家”进行深入诊断。

**当前活跃的专项专家 (仅限以下):**
{expertise_section}

**时间维度决策:**
- 你必须根据用户的提问（如“分析本周”、“分析1月1日到18日”）或者通过上下文感知来决定 `start_date` 和 `end_date`。
- 如果用户没有指定，默认可以不传，或者根据你的判断传最近7天。
- 将时间范围透传给下层工具函数。

**工作流程:**
1. **全局扫描**: 首先通过 `scan_campaigns_for_anomalies` 发现存在指标波动的广告系列。
2. **按需分派**: 如果扫描发现异常，且该活动属于你的职责范围：
   - **严格限制**: 你【只能】调用上述“当前活跃”列表中的专家工具 `analyze_specific_table`。
   - 如果某个表不在活跃列表中，严禁自行臆断或尝试调用。
3. **汇总报告**: 将专家的分析结果整合成一份专业的报告。

**原则:**
- 只有看到用户选择了某个表，你才会意识到有对应的专家可用。
- 输出必须专业、准确，使用中文。
- 深度分析结果必须准确标注来源（例如 "(数据来源: age 表)"）。
""")
            messages = [system_prompt] + messages
            
        response = self.llm_with_tools.invoke(messages)
        return {"messages": [response]}

    def should_continue(self, state: AgentState):
        messages = state['messages']
        last_message = messages[-1]
        if last_message.tool_calls:
            return "continue"
        return "end"

    async def chat_stream(self, message: str, messages: list, selected_tables: list = None):
        input_messages = []
        if messages:
            for msg in messages:
                if msg.role == 'user':
                    input_messages.append(HumanMessage(content=msg.content))
                elif msg.role == 'agent':
                    input_messages.append(AIMessage(content=msg.content))
        
        input_messages.append(HumanMessage(content=message))
        
        # Initialize state with selected tables
        initial_state = {
            "messages": input_messages,
            "selected_tables": selected_tables or []
        }
        
        async for event in self.app.astream_events(initial_state, version="v1"):
            kind = event["event"]
            
            # Stream LLM text output
            if kind == "on_chat_model_stream":
                content = event["data"]["chunk"].content
                if content:
                    yield content
            
            # Stream tool calls (show which sub-agent/tool is being called)
            elif kind == "on_tool_start":
                tool_name = event.get("name", "Unknown Tool")
                tool_input = event.get("data", {}).get("input", {})
                
                # Send a special marker for tool calls
                if tool_name == "scan_campaigns_for_anomalies":
                    yield f"\n\n🔍 **[调用工具]** 扫描所有广告系列...\n\n"
                elif tool_name == "call_pmax_agent":
                    campaign = tool_input.get("campaign_name", "Unknown")
                    yield f"\n\n🎯 **[调用 PMax Agent]** 分析 {campaign}...\n\n"
                elif tool_name == "analyze_specific_table":
                    campaign = tool_input.get("campaign_name", "Unknown")
                    table = tool_input.get("table_name", "Unknown")
                    yield f"\n\n🩺 **[专项分析]** 正在调遣专家分析 {campaign} 的 {table} 数据...\n\n"
                elif tool_name == "call_search_agent":
                    campaign = tool_input.get("campaign_name", "Unknown")
                    yield f"\n\n🔎 **[调用 Search Agent]** 分析 {campaign}...\n\n"
            
            # Stream tool results (optional, can show completion)
            elif kind == "on_tool_end":
                tool_name = event.get("name", "Unknown Tool")
                # You can optionally show tool completion
                # yield f"\n✅ [{tool_name}] 完成\n"

    def get_tables(self):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables

    def get_table_data(self, table_name, start_date: str = None, end_date: str = None):
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            pk_col = 'campaign' 
            if table_name == 'search_term': pk_col = 'search_term'
            elif table_name == 'product': pk_col = 'item_id' 
            elif table_name == 'asset': pk_col = 'ad_group' 
            elif table_name == 'audience': pk_col = 'audience_segment'
            elif table_name == 'channel': pk_col = 'channels'
            
            where_clause = ""
            params = []
            
            if start_date and end_date:
                where_clause = "WHERE t.date >= ? AND t.date <= ?"
                params = [start_date, end_date]
            elif start_date:
                where_clause = "WHERE t.date >= ?"
                params = [start_date]
            elif end_date:
                where_clause = "WHERE t.date <= ?"
                params = [end_date]
            
            query = f"""
                SELECT t.*, 
                       COALESCE(p.is_pinned, 0) as _pinned, 
                       COALESCE(p.display_order, 999999) as _order
                FROM {table_name} t
                LEFT JOIN user_preferences p 
                ON p.table_name = '{table_name}' AND p.item_identifier = t.{pk_col}
                {where_clause}
                ORDER BY _pinned DESC, _order ASC, date DESC
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            if not rows: return {"columns": [], "data": []}

            columns = [description[0] for description in cursor.description]
            display_columns = [c for c in columns if c not in ['_pinned', '_order']]
            
            data = [dict(row) for row in rows]
            return {"columns": display_columns, "data": data}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()

    def get_campaign_anomalies(self, target_date: str = None):
        """
        Identify anomalous campaigns for a specific date (defaults to latest in DB).
        """
        conn = get_db_connection()
        try:
            # 1. Determine the target "Today"
            if not target_date:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(date) FROM campaign")
                target_date = cursor.fetchone()[0]
                if not target_date:
                    return []
            
            # 2. Fetch raw data (Last 30 days relative to target_date)
            query = """
                SELECT date, campaign, roas, cpa, conversions 
                FROM campaign 
                WHERE date <= ? AND date >= date(?, '-45 days')
                ORDER BY campaign, date ASC
            """
            df = pd.read_sql_query(query, conn, params=(target_date, target_date))
            
            if df.empty:
                return []

            # Clean and Convert
            df['date'] = pd.to_datetime(df['date'])
            target_dt = pd.to_datetime(target_date)
            df['roas'] = pd.to_numeric(df['roas'], errors='coerce').fillna(0)
            df['cpa'] = pd.to_numeric(df['cpa'], errors='coerce').fillna(0)
            df['conversions'] = pd.to_numeric(df['conversions'], errors='coerce').fillna(0)

            anomalies = []
            
            # Group by campaign
            for campaign_name, group in df.groupby('campaign'):
                group = group.sort_values('date')
                if len(group) < 10: 
                    continue

                # The analysis target is target_dt
                last_date = target_dt
                
                # Check 3 days: T, T-1, T-2
                check_dates = [last_date - pd.Timedelta(days=i) for i in range(3)] 
                
                # Check Condition A: Efficiency for EACH of the last 3 days
                is_efficiency_bad = True
                
                for d in check_dates:
                    # Specific day row
                    day_row = group[group['date'] == d]
                    if day_row.empty:
                        is_efficiency_bad = False; break
                    
                    current_roas = day_row['roas'].values[0]
                    current_cpa = day_row['cpa'].values[0]

                    # History: 7 days prior to 'd' -> [d-7, d-1]
                    start_hist = d - pd.Timedelta(days=7)
                    end_hist = d - pd.Timedelta(days=1)
                    
                    hist_rows = group[(group['date'] >= start_hist) & (group['date'] <= end_hist)]
                    if hist_rows.empty:
                        is_efficiency_bad = False; break
                        
                    avg_roas = hist_rows['roas'].mean()
                    avg_cpa = hist_rows['cpa'].mean()
                    
                    # Criteria
                    roas_bad = (avg_roas > 0) and (current_roas < avg_roas * 0.8)
                    cpa_bad = (avg_cpa > 0) and (current_cpa > avg_cpa * 1.25)
                    
                    if not (roas_bad or cpa_bad):
                        is_efficiency_bad = False
                        break
                
                if not is_efficiency_bad:
                    continue

                # Check Condition B: No Growth
                # Current Period: [T-2, T]
                # Week-over-week Previous Period: [T-9, T-7]
                current_start = last_date - pd.Timedelta(days=2)
                prev_end = last_date - pd.Timedelta(days=7)
                prev_start = prev_end - pd.Timedelta(days=2)
                
                current_conv = group[(group['date'] >= current_start) & (group['date'] <= last_date)]['conversions'].sum()
                prev_conv = group[(group['date'] >= prev_start) & (group['date'] <= prev_end)]['conversions'].sum()
                
                growth = 0
                if prev_conv > 0:
                    growth = (current_conv - prev_conv) / prev_conv
                
                is_growth_bad = False
                if prev_conv > 0:
                     if growth <= 0: is_growth_bad = True
                else:
                     if current_conv == 0: is_growth_bad = True
                
                if is_growth_bad:
                    # Calculate summary stats for display (3d vs prev 7d)
                    curr_3d_mask = (group['date'] >= current_start) & (group['date'] <= last_date)
                    prev_7d_mask = (group['date'] >= last_date - pd.Timedelta(days=9)) & (group['date'] <= last_date - pd.Timedelta(days=3))
                    
                    curr_roas = group[curr_3d_mask]['roas'].mean()
                    prev_roas = group[prev_7d_mask]['roas'].mean()
                    
                    curr_cpa = group[curr_3d_mask]['cpa'].mean()
                    prev_cpa = group[prev_7d_mask]['cpa'].mean()

                    # Determine specific efficiency reason
                    efficiency_details = []
                    if prev_roas > 0 and curr_roas < prev_roas * 0.8:
                        drop_pct = (prev_roas - curr_roas) / prev_roas * 100
                        efficiency_details.append(f"ROAS -{drop_pct:.0f}%")
                    if prev_cpa > 0 and curr_cpa > prev_cpa * 1.25:
                        rise_pct = (curr_cpa - prev_cpa) / prev_cpa * 100
                        efficiency_details.append(f"CPA +{rise_pct:.0f}%")
                    
                    reason_str = " & ".join(efficiency_details)
                    if not reason_str: reason_str = "Efficiency Alert"

                    anomalies.append({
                        "id": str(campaign_name),
                        "campaign": campaign_name,
                        "date": last_date.strftime('%Y-%m-%d'),
                        "growth_rate": growth,
                        "current_conv": float(current_conv),
                        "prev_conv": float(prev_conv),
                        # Efficiency Metrics
                        "curr_roas": float(curr_roas) if not pd.isna(curr_roas) else 0.0,
                        "prev_roas": float(prev_roas) if not pd.isna(prev_roas) else 0.0,
                        "curr_cpa": float(curr_cpa) if not pd.isna(curr_cpa) else 0.0,
                        "prev_cpa": float(prev_cpa) if not pd.isna(prev_cpa) else 0.0,
                        
                        "status": "Critical",
                        "reason": f"{reason_str} & No Growth"
                    })
            
            return anomalies

        except Exception as e:
            print(f"Anomaly Detection Error: {e}")
            return []
        finally:
            conn.close()

    def update_preference(self, table_name: str, item_identifier: str, is_pinned: int = None, display_order: int = None):
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM user_preferences WHERE table_name=? AND item_identifier=?", (table_name, item_identifier))
            exists = cursor.fetchone()
            
            if exists:
                if is_pinned is not None:
                    cursor.execute("UPDATE user_preferences SET is_pinned=? WHERE table_name=? AND item_identifier=?", (is_pinned, table_name, item_identifier))
                if display_order is not None:
                    cursor.execute("UPDATE user_preferences SET display_order=? WHERE table_name=? AND item_identifier=?", (display_order, table_name, item_identifier))
            else:
                pinned = is_pinned if is_pinned is not None else 0
                order = display_order if display_order is not None else 0
                cursor.execute("INSERT INTO user_preferences (table_name, item_identifier, is_pinned, display_order) VALUES (?, ?, ?, ?)", (table_name, item_identifier, pinned, order))
            
            conn.commit()
            return {"status": "success"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()

    def reset_preferences(self, table_name: str):
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM user_preferences WHERE table_name=?", (table_name,))
            conn.commit()
            return {"status": "success"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()

    def get_campaign_details(self, campaign_name: str, start_date: str = None, end_date: str = None):
        """Get all related data for a specific campaign from all tables"""
        tables = [
            'search_term', 'channel', 'asset', 
            'audience', 'age', 'gender', 
            'location_by_cities_all_campaign', 'ad_schedule'
        ]
        
        result = {}
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            for table in tables:
                try:
                    where_conditions = []
                    params = []

                    # 1. Campaign Filter (Skip for product)
                    if table != 'product':
                        campaign_col = 'campaign'
                        if table == 'channel':
                            campaign_col = 'campaigns'
                        where_conditions.append(f"{campaign_col} = ?")
                        params.append(campaign_name)
                    
                    # 2. Date Filter
                    if start_date:
                        where_conditions.append("date >= ?")
                        params.append(start_date)
                    if end_date:
                        where_conditions.append("date <= ?")
                        params.append(end_date)
                    
                    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                    
                    query = f"SELECT * FROM {table}{where_clause}"
                    
                    # Check for sort column
                    check_query = f"PRAGMA table_info({table})"
                    cursor.execute(check_query)
                    cols = [info[1] for info in cursor.fetchall()]
                    
                    if 'cost' in cols:
                        query += " ORDER BY date DESC, CAST(cost AS REAL) DESC"
                    else:
                        query += " ORDER BY date DESC"
                    
                    cursor.execute(query, tuple(params))
                    
                    rows = cursor.fetchall()
                    
                    if rows:
                        columns = [description[0] for description in cursor.description]
                        data = [dict(row) for row in rows]
                        result[table] = {"columns": columns, "data": data}
                    else:
                        result[table] = {"columns": [], "data": []}
                except Exception as e:
                    result[table] = {"error": str(e), "columns": [], "data": []}
            
            return result
        finally:
            conn.close()
