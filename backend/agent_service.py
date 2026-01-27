import os
import sqlite3
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any, TypedDict, Annotated
import operator
import json
import uuid
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool, InjectedToolCallId
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode

from expert_system import ContextGuard  # ExpertEngine removed - now using pure LLM analysis

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
    streaming=True,
    request_timeout=60 # Force fallback if thinking takes too long
)

# --- Table Expert Knowledge ---
# This dict provides specific metric descriptions and diagnostic focus for the sub-agent
TABLE_EXPERT_KNOWLEDGE = {
    "Anomalies": {
        "title": "Anomaly Guard (异常巡检专家)",
        "focus": "全账户自动巡检，识别效率异常与增长停滞。",
        "metrics_desc": "campaign: 系列名, roas: 广告支出回报率, cpa: 单次转化成本, conversions: 转化数",
        "expert_rules": """
        - **ROAS 崩盘**: 3天 ROAS < 账户均值 80% -> 触发深度诊断
        - **CPA 飙升**: 3天 CPA > 账户均值 125% -> 触发成本异常警报
        - **增长停滞**: 7天同比转化零增长 -> 标记增长风险
        - **风控保护**: 大促期/冷启动期自动降级风险动作
        """
    },
    "Campaigns": {
        "title": "Campaign Manager (广告系列管理专家)",
        "focus": "广告系列整体健康度评估与优化建议。",
        "metrics_desc": "campaign: 名称, cost: 消耗, conversions: 转化, roas: ROAS, cpa: CPA",
        "expert_rules": """
        - **预算效率**: 预算消耗率 < 70% -> 检查定向或出价
        - **转化质量**: 转化价值/转化数 持续下降 -> 检查落地页或受众
        - **系列结构**: 同系列广告组数量 > 10 -> 建议精简结构
        """
    },
    "Products": {
        "title": "Product Specialist (产品专家)",
        "focus": "商品/SKU 层级效率分析。",
        "metrics_desc": "product_title: 商品名, item_id: SKU, cost: 消耗, conversions: 转化, roas: ROAS",
        "expert_rules": """
        - **僵尸商品**: 消耗 > $80 且 0 转化 -> 建议排除
        - **冷启动保护**: 新品消耗 < $30 -> 暂不优化
        - **预算霸占**: 单品占预算 > 85% -> 警告测试饥饿风险
        """
    },
    "asset": {
        "title": "Creative Asset Expert (素材创意专家)",
        "focus": "创意素材效果评估与轮换建议。",
        "metrics_desc": "asset_name: 素材名, asset_type: 类型, cost: 消耗, conversions: 转化, ctr: 点击率",
        "expert_rules": """
        - **疲劳检测**: CTR 连续 7 天下降 > 20% -> 建议更换素材
        - **效果分层**: 按 ROAS 分为 Top/Middle/Bottom 三档
        - **格式建议**: 视频素材 CTR 通常高于静态图，注意对比
        """
    },
    "location": {
        "title": "Location & Geo Expert (地域专家)",
        "focus": "地理位置投放效率分析。",
        "metrics_desc": "location: 位置, cost: 消耗, conversions: 转化, roas: ROAS",
        "expert_rules": """
        - **黑洞检测**: 消耗 >= $100 且 0 转化 -> 建议排除
        - **效率风险**: CPA >= 2x 均值 -> 建议降低出价 30%
        - **观察期**: 消耗 < $50 或 点击 < 50 -> 数据不足，继续观察
        """
    },
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
    if table_name == 'channel': 
        campaign_col = 'campaigns'
    elif table_name == 'product':
        campaign_col = '1' # Special case: product table lacks campaign pivot, disable filter

    # 2. Fetch Targeted Table Data with Date Filter
    where_conditions = []
    params = []
    
    if table_name != 'product':
        where_conditions.append(f"{campaign_col} = ?")
        params.append(campaign_name)
    
    if start_date:
        where_conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        where_conditions.append("date <= ?")
        params.append(end_date)
        
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    query = f"SELECT * FROM {table_name} WHERE {where_clause} ORDER BY CAST(cost AS REAL) DESC LIMIT 15"
    table_data = query_db(query, tuple(params))
    
    if not table_data:
        return f"Found no data in '{table_name}' for campaign '{campaign_name}'."

    # 3. Load Custom Rules for this Table (if any) - 用户自定义规则优先
    analysis_rules = ""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT rule_prompt FROM agent_custom_rules WHERE table_name = ? AND is_active = 1", (table_name,))
        result = cursor.fetchone()
        if result and result[0]:
            # 用户有自定义规则，使用自定义规则
            analysis_rules = result[0]
        conn.close()
    except Exception as e:
        print(f"Warning: Could not load custom rules for {table_name}: {e}")

    # 如果没有自定义规则，使用默认规则
    if not analysis_rules:
        expert = TABLE_EXPERT_KNOWLEDGE.get(table_name, {})
        analysis_rules = expert.get('expert_rules', '分析数据效率和异常。')

    # 4. 获取专家信息
    expert = TABLE_EXPERT_KNOWLEDGE.get(table_name, {
        "title": f"{table_name} 分析专家",
        "focus": "数据效率分析",
        "metrics_desc": "请根据数据列自行判断指标含义"
    })

    # 5. 纯 LLM 分析 - 提示词即规则
    prompt = f"""
    你是专业的 Google Ads 优化专家，专注于: {expert['title']}。
    
    **分析目标**: {expert['focus']}
    **指标说明**: {expert['metrics_desc']}
    
    ---
    ## ⚠️ 重要：你必须严格按照以下规则进行分析
    
    {analysis_rules}
    
    ---
    **广告系列**: {campaign_name}
    **分析周期**: {start_date or 'ALL'} 至 {end_date or 'ALL'}
    **整体基准**: {context_str}
    
    **原始数据 (按重要性排序):**
    {safe_truncate_data(table_data, MAX_CONTEXT_CHARACTERS)}
    
    ---
    **输出要求**:
    1. **严格执行规则**: 按照上述【分析规则】中的阈值和条件进行判断
    2. **状态与置信度**: 明确标注 "状态: [观察期 | 可行动]" 和 "置信度: [高|中|低]"
    3. **具体行动建议**: 给出具体的优化建议（如排除词、调整出价等）
    4. 输出格式：简洁的 Markdown，使用中文
    """

    res = sub_llm.invoke(prompt)
    return res.content


def scan_campaigns_for_anomalies(target_date: str = None) -> str:
    """
    Scans for anomalies using the robust 3-Day Logic (User Defined) and Expert Diagnosis.
    
    Workflow:
    1. Trigger: Detect continuity & growth issues.
    2. Guard: Check business context (Promotion, Cold Start).
    3. Experts: Run specialized diagnostic engines (SearchTerm, Channel, Product, Geo).
    4. Report: Aggregate findings into a decision-grade report.
    """
    # 1. Main Agent Trigger
    anomalies = get_campaign_anomalies_logic(target_date) 
    
    if not anomalies:
         return "✅ 效率巡检通过：没有广告系列触发 3 天 ROAS/CPA 预警。"

    # 加载异常检测规则 (用户自定义或默认)
    analysis_rules = ""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT rule_prompt FROM agent_custom_rules WHERE table_name = 'Anomalies' AND is_active = 1")
        result = cursor.fetchone()
        if result and result[0]:
            analysis_rules = result[0]
        conn.close()
    except:
        pass
    
    if not analysis_rules:
        expert = TABLE_EXPERT_KNOWLEDGE.get("Anomalies", {})
        analysis_rules = expert.get("expert_rules", "")

    # 准备数据给 LLM
    report = []
    for a in anomalies:
        campaign_name = a.get('campaign', 'Unknown')
        campaign_type = a.get('campaign_type', 'Unknown')
        
        # 收集相关数据供 LLM 分析
        related_data = {}
        
        # 搜索词数据
        st_data = query_db("SELECT * FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        if st_data:
            related_data['search_term'] = st_data
        
        # 渠道数据 (PMax)
        ch_data = query_db("SELECT * FROM channel WHERE campaigns LIKE ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (f"%{campaign_name}%",))
        if ch_data:
            related_data['channel'] = ch_data
        
        # 商品数据
        pr_data = query_db("SELECT * FROM product ORDER BY CAST(cost AS REAL) DESC LIMIT 10")
        if pr_data:
            related_data['product'] = pr_data
        
        # 地域数据
        geo_data = query_db("SELECT * FROM location_by_cities_all_campaign WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        if geo_data:
            related_data['geo'] = geo_data

        # 纯 LLM 分析
        prompt = f"""
        你是 Google Ads 全账户巡检专家。请严格按照以下规则分析异常广告系列。
        
        ---
        ## ⚠️ 必须遵守的分析规则
        
        {analysis_rules}
        
        ---
        ## 触发异常的广告系列
        
        **系列名称**: {campaign_name}
        **系列类型**: {campaign_type}
        **异常数据**: {json.dumps(a, ensure_ascii=False)}
        
        ## 相关维度数据
        
        {json.dumps(related_data, ensure_ascii=False, indent=2)[:8000]}
        
        ---
        ## 输出要求
        
        1. **根本原因分析**: 判断效率下降的根本原因（流量质量、结构问题、市场变化）
        2. **严格执行规则**: 按照上述规则中的阈值进行判断
        3. **具体行动建议**: 给出可立即执行的优化建议
        4. **置信度标注**: 标注分析的置信度 [高|中|低]
        5. 输出格式：简洁 Markdown，中文
        """
        
        llm_analysis = sub_llm.invoke(prompt).content
        report.append(f"### ⚠️ {campaign_name}\n\n{llm_analysis}\n")

    return "\n".join(report)


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
        
        total_pmax_cost = sum([float(c.get('cost', 0) or 0) for c in channels_data])
        
        if channels_data:
            report.append("#### 📡 A. Channel Analysis (Data Source: channel table)")
            
            # --- PMax Traffic Washing Logic ---
            display_spend = 0
            display_roas = 0
            video_spend = 0
            
            for c in channels_data:
                channel_name = c.get('channels', 'Unknown')
                cost = float(c.get('cost', 0) or 0)
                val = float(c.get('conv_value', 0) or 0)
                
                if 'Display' in channel_name:
                    display_spend += cost
                    display_roas = (val / cost) if cost > 0 else 0
                if 'Video' in channel_name:
                    video_spend += cost
            
            display_share = (display_spend / total_pmax_cost * 100) if total_pmax_cost > 0 else 0
            
            # Logic 1: Display Waste Check
            if display_share > 35 and display_roas < 1.0:
                 report.append(f"⚠️ **PMax 流量洗样判定 (Traffic Washing Detected)**")
                 report.append(f"   - **现象**: Display Channel is consuming {display_share:.1f}% of budget with low ROAS ({display_roas:.2f}).")
                 report.append("   - **专家经验**: 若 Display 消耗占比 > 35% 且其 ROAS < 全账户均值 50%，判定为 PMax 正在吞噬低质流量 (PMax is dumping budget into cheap inventory).")
                 report.append("   - **建议**: Consider tightening audience signals or excluding placements.")
            
            # Logic 2: Cross-Channel Check
            for c in channels_data:
                channel_name = c.get('channels', 'Unknown')
                cost = float(c.get('cost', 0) or 0)
                val = float(c.get('conv_value', 0) or 0)
                roas = (val / cost) if cost > 0 else 0.0
                
                metrics_str = f"Cost: ${cost:.2f} | ROAS: {roas:.2f}"
                report.append(f"- **{channel_name}**: {metrics_str}")

            report.append("")
        else:
            report.append("ℹ️ No active channel data found for this campaign.")
            report.append("")

    except Exception as e:
        report.append(f"Error in Channel Analysis: {e}")

    # B. Product Analysis (The "Shelf")
    # Logic: Structural Profitability Check
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
            
            # Cold Start Protection (Simulated): If cost < 30, ignore unless 0 conv for long time (not checked here)
            if cost > 30: 
                if cost > 50 and conv == 0:
                    zombies.append(f"{title} (ID: {item_id}) - Cost ${cost:.2f}, 0 Conv")
                elif cost > 20 and roas < 0.5:
                    inefficient.append(f"{title} (ID: {item_id}) - ROAS {roas:.2f}")

        report.append("#### 📦 B. Product Analysis (Source: product table)")
        
        if zombies:
            report.append("❌ **Zombie Products (High Cost, 0 Conv)**:")
            for z in zombies: report.append(f"  - {z}")
        if inefficient:
            report.append("⚠️ **结构性损耗判定 (Structural Profit Issue)**:")
            for i in inefficient: report.append(f"  - {i}")
            report.append("   - **专家经验**: 若低 ROAS 商品集中在低毛利 SKU，判定为 结构性毛利问题而非单纯流量问题 (Structural margin issue, not just traffic).")
            report.append("   - **注意**: Unless these are high-margin (>80%) traffic drivers, they are bleeding profit.")
        
        if not zombies and not inefficient:
             report.append("✅ Top spending products are performing within acceptable range.")
        
        report.append("")

    except Exception as e:
        report.append(f"Error in Product Analysis: {e}")

    # C. Location Analysis
    try:
        locs = query_db("SELECT matched_location, cost, conversions FROM location_by_cities_all_campaign WHERE campaign = ? AND CAST(cost AS REAL) > 50 AND CAST(conversions AS REAL) = 0 ORDER BY CAST(cost AS REAL) DESC LIMIT 3", (campaign_name,))
        report.append("#### 🌍 C. Location Analysis")
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
    try:
        bad_keywords = ['free', 'repair', 'login', 'support', 'manual', 'review']
        terms = query_db("SELECT search_term, cost, conversions FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 20", (campaign_name,))
        
        found_bad_terms = []
        for t in terms:
            term = t.get('search_term', '').lower()
            cost = float(t.get('cost', 0))
            if any(bk in term for bk in bad_keywords):
                found_bad_terms.append(f"'{term}' (Cost ${cost})")
        
        report.append("#### 🔍 D. Search Term Analysis")
        if found_bad_terms:
            report.append("⚠️ **Irrelevant Search Terms Detected**:")
            for ft in found_bad_terms: report.append(f"- {ft}")
            report.append("👉 **Action**: Add these as Account-Level Negative Keywords.")
        else:
            report.append("✅ No obvious junk keywords found in top spenders.")
            
    except Exception as e:
        report.append(f"Error in Search Term Analysis: {e}")

    return "\n".join(report)


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

    # B. Match Types (Broad vs Exact Logic)
    try:
        match_stats = query_db("""
            SELECT match_type, SUM(CAST(cost AS REAL)) as total_cost, SUM(CAST(conversions AS REAL)) as total_conv, SUM(CAST(conv_value AS REAL)) as total_value
            FROM search_term WHERE campaign = ? GROUP BY match_type ORDER BY total_cost DESC
        """, (campaign_name,))
        
        # Calculate CVR for Flash to use
        enhanced_stats = []
        for ms in match_stats:
            d = dict(ms)
            cost = d['total_cost'] or 0
            conv = d['total_conv'] or 0
            # Rough CVR estimation requires clicks, but we assume low CVR if High Cost Low Conv
            d['cpa'] = cost / conv if conv > 0 else 0
            enhanced_stats.append(d)
            
        data_context.append(f"\n[Match Type Stats]: {json.dumps(enhanced_stats, ensure_ascii=False)}")
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

    **Analysis Logic (Strictly apply Expert Experience):**
    1. **搜索流量质量判定 (Search Quality)**: 
       - If 'Broad' match type has > 40% spend share AND its CPA is > 1.5x of 'Exact' match, VERDICT: "判定为 流量匹配质量下滑 (Match Quality Degradation)".
       - Logic: "若广泛匹配占比提升且 Search Term CVR 同期下降，判定为 流量匹配质量下滑".
    2. **Search Terms**: Identify irrelevant junk terms (negative opportunities).
    3. **Audience**: Identify high-spend (> $50) audiences with 0 conversions.

    **Output Format (Markdown):**
    ### 🔍 Search Campaign Deep Dive: {campaign_name}
    #### 1. 核心发现 (Core Findings)
    - **专家判定**: (Cite the Expert Verdict if applicable, e.g. "流量匹配质量下滑")
    - **Evidence**: (Data backing the verdict)
    
    #### 2. 详细分析 (Analysis)
    - **匹配类型效能**: (Compare Broad vs Exact/Phrase)
    - **搜索词**: ...
    
    #### 3. 优化建议 (Actions)
    - [ ] Action 1
    """

    msg = sub_llm.invoke(prompt)
    return msg.content


# --- LangGraph Setup ---

class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], operator.add]
    selected_tables: List[str]


# --- Standalone Logic (Decoupled from AgentService) ---

def get_campaign_anomalies_logic(target_date: str = None):
    """
    Identify anomalous campaigns for a specific date (defaults to latest in DB).
    Risk Control: Returns empty if target_date falls within major promotion periods.
    """
    # --- 0. Risk Control: Promotion Protection Defaults ---
    # Format: (Start, End). Example: Black Friday / Cyber Monday.
    PROMOTION_PERIODS = [
        ('2025-11-20', '2025-12-05'), # BFCM
        ('2026-06-01', '2026-06-20'), # 618 Sale
    ]
    
    conn = get_db_connection()
    try:
        # 1. Determine the target "Today"
        if not target_date:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(date) FROM campaign")
            res = cursor.fetchone()
            target_date = res[0] if res else None
            if not target_date:
                return []
        
        # 2. Fetch raw data (Last 45 days relative to target_date)
        query = """
            SELECT date, campaign, roas, cpa, conversions, budget, campaign_type 
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

                # 4. Integrate Context Guard Risk Assessment
                risk_info = ContextGuard.check_risk({"campaign": campaign_name}, last_date.strftime('%Y-%m-%d'))
                
                risk_label = "🔴 Critical"
                if risk_info['status'] == "BLOCK": risk_label = "🛡️ Protected (Tag Only)"
                elif risk_info['status'] == "MARK": risk_label = "⚠️ Warning (Observing)"

                # 5. Get Campaign Type for Expert Routing
                camp_type = group['campaign_type'].iloc[0] if 'campaign_type' in group.columns else 'Unknown'
                
                # Determine suggested experts based on campaign type
                suggested_experts = []
                if 'search' in str(camp_type).lower():
                    suggested_experts = ['search_term', 'keyword', 'age', 'gender']
                elif 'pmax' in str(camp_type).lower() or 'performance max' in str(camp_type).lower():
                    suggested_experts = ['channel', 'product', 'location_by_cities_all_campaign']
                else:
                    suggested_experts = ['age', 'gender', 'location_by_cities_all_campaign']

                anomalies.append({
                    "id": str(campaign_name),
                    "campaign": campaign_name,
                    "campaign_type": str(camp_type),
                    "date": last_date.strftime('%Y-%m-%d'),
                    "growth_rate": growth,
                    "current_conv": float(current_conv),
                    "prev_conv": float(prev_conv),
                    # Efficiency Metrics
                    "curr_roas": float(curr_roas) if not pd.isna(curr_roas) else 0.0,
                    "prev_roas": float(prev_roas) if not pd.isna(prev_roas) else 0.0,
                    "curr_cpa": float(curr_cpa) if not pd.isna(curr_cpa) else 0.0,
                    "prev_cpa": float(prev_cpa) if not pd.isna(prev_cpa) else 0.0,
                    
                    "status": risk_label,
                    "risk_level": risk_info['status'],
                    "guard_reasons": risk_info['reasons'],
                    "suggested_experts": suggested_experts,
                    "reason": f"{reason_str} & No Growth"
                })
        
        return anomalies

    except Exception as e:
        print(f"Anomaly Detection Error: {e}")
        return []
    finally:
        conn.close()


def get_product_anomalies_logic(target_date: str = None):
    """
    Identify anomalous products for a specific date (defaults to latest in DB).
    Similar logic to campaign anomalies but adapted for product metrics.
    
    Detection criteria:
    1. High cost with low/no clicks (Zombie Products)
    2. CTR declining trend (3-day consecutive decline)
    3. Cost efficiency degradation
    """
    conn = get_db_connection()
    try:
        # 1. Determine the target "Today"
        if not target_date:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(date) FROM product")
            res = cursor.fetchone()
            target_date = res[0] if res else None
            if not target_date:
                return []
        
        # 2. Fetch raw data (Last 14 days relative to target_date)
        query = """
            SELECT date, title, item_id, cost, clicks, impr, ctr, avg_cpc
            FROM product 
            WHERE date <= ? AND date >= date(?, '-14 days')
            ORDER BY item_id, date ASC
        """
        df = pd.read_sql_query(query, conn, params=(target_date, target_date))
        
        if df.empty:
            return []

        # Clean and Convert
        df['date'] = pd.to_datetime(df['date'])
        target_dt = pd.to_datetime(target_date)
        df['cost'] = pd.to_numeric(df['cost'].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
        df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce').fillna(0)
        df['impr'] = pd.to_numeric(df['impr'], errors='coerce').fillna(0)
        df['ctr'] = pd.to_numeric(df['ctr'].astype(str).str.replace('%', ''), errors='coerce').fillna(0)

        anomalies = []
        
        # Group by product (item_id)
        for item_id, group in df.groupby('item_id'):
            group = group.sort_values('date')
            if len(group) < 3:
                continue

            title = group['title'].iloc[-1] if 'title' in group.columns else 'Unknown Product'
            last_date = target_dt
            
            # Get last 3 days data
            last_3_days = group[group['date'] >= (last_date - pd.Timedelta(days=2))]
            if last_3_days.empty:
                continue
                
            # Get previous 7 days for comparison
            prev_7_days = group[(group['date'] >= (last_date - pd.Timedelta(days=9))) & 
                                (group['date'] <= (last_date - pd.Timedelta(days=3)))]
            
            # Current period metrics
            curr_cost = last_3_days['cost'].sum()
            curr_clicks = last_3_days['clicks'].sum()
            curr_ctr = last_3_days['ctr'].mean()
            
            # Previous period metrics  
            prev_cost = prev_7_days['cost'].sum() if not prev_7_days.empty else 0
            prev_clicks = prev_7_days['clicks'].sum() if not prev_7_days.empty else 0
            prev_ctr = prev_7_days['ctr'].mean() if not prev_7_days.empty else 0
            
            # Anomaly Detection Rules
            reasons = []
            
            # Rule 1: Zombie Product (High cost, no clicks)
            if curr_cost > 30 and curr_clicks == 0:
                reasons.append(f"Zombie Product (Cost ${curr_cost:.2f}, 0 Clicks)")
            
            # Rule 2: CTR Decline > 30%
            if prev_ctr > 0 and curr_ctr < prev_ctr * 0.7:
                decline_pct = (prev_ctr - curr_ctr) / prev_ctr * 100
                reasons.append(f"CTR -{decline_pct:.0f}%")
            
            # Rule 3: Cost Efficiency Degradation (cost up, clicks down)
            if prev_cost > 0 and prev_clicks > 0:
                prev_cpc = prev_cost / prev_clicks
                curr_cpc = curr_cost / curr_clicks if curr_clicks > 0 else float('inf')
                if curr_cpc > prev_cpc * 1.5 and curr_cost > 20:
                    reasons.append(f"CPC +{((curr_cpc - prev_cpc) / prev_cpc * 100):.0f}%")
            
            if reasons:
                anomalies.append({
                    "id": str(item_id),
                    "item_id": str(item_id),
                    "title": str(title)[:50],  # Truncate long titles
                    "date": last_date.strftime('%Y-%m-%d'),
                    "curr_cost": float(curr_cost),
                    "prev_cost": float(prev_cost),
                    "curr_clicks": float(curr_clicks),
                    "prev_clicks": float(prev_clicks),
                    "curr_ctr": float(curr_ctr) if not pd.isna(curr_ctr) else 0.0,
                    "prev_ctr": float(prev_ctr) if not pd.isna(prev_ctr) else 0.0,
                    "reason": " & ".join(reasons)
                })
        
        # Sort by cost (highest cost issues first)
        anomalies.sort(key=lambda x: x['curr_cost'], reverse=True)
        return anomalies[:20]  # Limit to top 20

    except Exception as e:
        print(f"Product Anomaly Detection Error: {e}")
        return []
    finally:
        conn.close()


class AgentService:
    def __init__(self):
        print(f"Initializing Main Agent with model={MAIN_MODEL_NAME}")
        self.llm = main_llm
        
        self.tools = [scan_campaigns_for_anomalies, analyze_specific_table, call_pmax_agent, call_search_agent]
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        
        self._init_prefs_db()

        workflow = StateGraph(AgentState)
        workflow.add_node("agent", self.call_model)
        workflow.add_node("tools", self.call_tools) # Use custom node
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

    def call_tools(self, state: AgentState):
        """
        Manual execution of tools to bypass ToolNode strictness.
        This ensures we can flexibly handle return types and injection.
        """
        messages = state['messages']
        last_message = messages[-1]
        
        if not hasattr(last_message, 'tool_calls') or not last_message.tool_calls:
            return {"messages": []}
        
        outputs = []
        
        for tool_call in last_message.tool_calls:
            tool_name = tool_call.get('name')
            tool_args = tool_call.get('args', {})
            tool_id = tool_call.get('id')
            
            # Robust Fallback for ID
            if not tool_id:
                print(f"⚠️ Warning: Missing tool_call_id for {tool_name}, generating random one.")
                tool_id = str(uuid.uuid4())
            
            print(f"🔧 Executing Tool: {tool_name} (ID: {tool_id})")
            
            result = "Error: Tool not found."
            
            try:
                if tool_name == 'scan_campaigns_for_anomalies':
                    result = scan_campaigns_for_anomalies(**tool_args)
                    
                elif tool_name == 'analyze_specific_table':
                    result = analyze_specific_table(**tool_args)
                
                elif tool_name == 'call_pmax_agent':
                    result = call_pmax_agent(**tool_args)

                elif tool_name == 'call_search_agent':
                    result = call_search_agent(**tool_args)

                else:
                    result = f"Error: Unknown tool '{tool_name}'."
                    
            except Exception as e:
                print(f"❌ Tool Execution Error [{tool_name}]: {e}")
                result = f"Error executing tool {tool_name}: {str(e)}"
            
            # Ensure result is string
            if not isinstance(result, str):
                result = str(result)
            
            outputs.append(ToolMessage(content=result, tool_call_id=tool_id))
            
        return {"messages": outputs}

    def _sanitize_history(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """
        Converts previous Tool interactions into plain text to avoid strict 
        'thought_signature' checks by Gemini 3.0 on historical messages.
        Only the LAST message is kept as-is (if it's a User Request).
        """
        sanitized = []
        for i, msg in enumerate(messages):
            # Keep the System Prompt
            if isinstance(msg, SystemMessage):
                sanitized.append(msg)
                continue
                
            # Flatten Tool Interactions
            if isinstance(msg, AIMessage) and msg.tool_calls:
                # Convert 'AI calling tool' to text
                tool_names = [t['name'] for t in msg.tool_calls]
                sanitized.append(AIMessage(content=f"🤔 [Thinking History] I decided to call tools: {', '.join(tool_names)}."))
            
            elif isinstance(msg, ToolMessage):
                # Convert 'Tool Output' to text
                # Truncate very long outputs to save context
                content_preview = str(msg.content)[:500] + "..." if len(str(msg.content)) > 500 else str(msg.content)
                sanitized.append(HumanMessage(content=f"🔧 [Tool Output History]: {content_preview}"))
            
            else:
                # Keep normal text messages (User/AI Chat)
                sanitized.append(msg)
                
        return sanitized

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

        # Ensure System Prompt
        if not isinstance(messages[0], SystemMessage):
            system_prompt = SystemMessage(content=f"""你是 AdsManager Main Agent (任务调度器)。
你的职责是实时监控广告表现，并协调“专项专家”进行深入诊断。

**当前活跃的专项专家 (仅限以下):**
{expertise_section}

**时间维度决策:**
- 你必须根据用户的提问（如“分析本周”、“分析1月1日到18日”）或者通过上下文感知来决定 `start_date` 和 `end_date`。
- 如果用户没有指定，默认使用数据截止日期（如 2026-01-18）的前7天。
- 将时间范围透传给下层工具函数。

**工作流程与汇报原则:**
1. **显性化思考**: 在调用任何工具前，你必须先输出一段分析思路（例如：“监测到结果...我将启动...”）。
2. **全量扫描与汇报**: 
   - 当调用 `scan_campaigns_for_anomalies` 时，如果返回结果包含多个广告系列，你【必须】在汇总汇报中涵盖【所有】被识别出的异常系列。严禁只保留一个或过度简化。
3. **多维专家调度**: 
   - 对于每一个被检测出的异常系列，你应当根据其“初步核心原因”（Root Cause）决定调遣哪些专家。
   - 如果一个系列同时存在主词损耗和人群偏差，你应当在一个轮次内同时启动对应的多个专家工具 `analyze_specific_table`（如 search_term + age + gender）。
4. **汇总报告**: 将所有专家的深度分析结论进行聚合，生成专业、结构化且全中文化的最终总结。

**原则:**
- 只有看到用户勾选了某个表对应的 Agent，你才具备调遣该专家的权限。
- 输出必须专业、准确。深度分析结果必须准确标注数据来源（例如 "(数据来源: channel 表)"）。
- **透明化执行**: 用户需要看到你对每一个异常系列的专家分派过程。
""")
            messages = [system_prompt] + messages
            
        # SANITIZE HISTORY: Bypass 'thought_signature' check for past turns
        # We only strictly need structured objects for the *current* turn if we are processing it.
        # But here, we are invoking the model to *generate* the next step.
        # So previous steps can be flattened.
        safe_messages = self._sanitize_history(messages)

        print(f"🤖 Invoking Main Model ({MAIN_MODEL_NAME}) with {len(safe_messages)} safe messages...")
        response = self.llm_with_tools.invoke(safe_messages)
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
        """Wrapper for standalone logic"""
        return get_campaign_anomalies_logic(target_date)

    def get_product_anomalies(self, target_date: str = None):
        """Wrapper for product anomaly detection"""
        return get_product_anomalies_logic(target_date)

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

    def _init_custom_rules_db(self):
        """Initialize the custom rules table if it doesn't exist"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_custom_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                rule_prompt TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def save_custom_rule(self, table_name: str, rule_prompt: str):
        """Save or update a custom rule"""
        self._init_custom_rules_db()
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Check if rule already exists for this table
            cursor.execute("SELECT id FROM agent_custom_rules WHERE table_name = ?", (table_name,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing rule
                cursor.execute("""
                    UPDATE agent_custom_rules 
                    SET rule_prompt = ?, updated_at = CURRENT_TIMESTAMP 
                    WHERE table_name = ?
                """, (rule_prompt, table_name))
            else:
                # Insert new rule
                cursor.execute("""
                    INSERT INTO agent_custom_rules (table_name, rule_prompt) 
                    VALUES (?, ?)
                """, (table_name, rule_prompt))
            
            conn.commit()
            return {"status": "success", "message": "Custom rule saved"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            conn.close()

    def get_custom_rules(self, table_name: str):
        """Get custom rules for a specific table"""
        self._init_custom_rules_db()
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT rule_prompt, created_at, updated_at 
                FROM agent_custom_rules 
                WHERE table_name = ? AND is_active = 1
            """, (table_name,))
            result = cursor.fetchone()
            
            if result:
                return {
                    "rule_prompt": result[0],
                    "created_at": result[1],
                    "updated_at": result[2]
                }
            else:
                return {"rule_prompt": None}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()

    def get_agent_default_prompt(self, table_name: str):
        """Get the default prompt/rules for a specific agent from TABLE_EXPERT_KNOWLEDGE"""
        if table_name in TABLE_EXPERT_KNOWLEDGE:
            knowledge = TABLE_EXPERT_KNOWLEDGE[table_name]
            return {
                "title": knowledge.get("title", ""),
                "focus": knowledge.get("focus", ""),
                "metrics_desc": knowledge.get("metrics_desc", ""),
                "expert_rules": knowledge.get("expert_rules", "").strip(),
                "default_prompt": f"""【{knowledge.get("title", "")}】
专注领域: {knowledge.get("focus", "")}
指标说明: {knowledge.get("metrics_desc", "")}

专家规则:
{knowledge.get("expert_rules", "").strip()}"""
            }
        else:
            return {"error": f"Unknown agent: {table_name}", "default_prompt": ""}

