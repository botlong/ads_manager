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

# Initialize LLMs Globally
main_llm = ChatOpenAI(
    model=MAIN_MODEL_NAME,
    base_url=BASE_URL,
    api_key=API_KEY,
    temperature=0,
    streaming=True
)

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

# --- Tools Definition ---

@tool
def scan_campaigns_for_anomalies() -> str:
    """
    Scans the 'campaign' table to identify anomalies.
    Rules (Matching Frontend Logic):
    1. ROAS Drop: Current ROAS < Previous ROAS * 0.8 (20% drop)
    2. CPA Rise: Current CPA > Previous CPA * 1.25 (25% rise)
    Returns a JSON string list of anomalies with Campaign Name and Type.
    """
    query = """
        SELECT campaign, campaign_type, roas, roascompare_to, cost_conv, cost_conv_compare_to, conversions
        FROM campaign
    """
    rows = query_db(query)
    anomalies = []
    
    for row in rows:
        try:
            conv = float(row['conversions']) if row['conversions'] else 0
            if conv < 3: continue
        except: continue

        c_name = row['campaign']
        c_type = row['campaign_type']
        if not c_name or c_name in ['--', 'Total', 'None']: continue

        issues = []
        
        try:
            roas = float(row['roas']) if row['roas'] else 0
            roas_comp = float(row['roascompare_to']) if row['roascompare_to'] else 0
            if roas_comp > 0 and roas < (roas_comp * 0.8):
                issues.append(f"ROAS Drop ({(roas_comp - roas)/roas_comp*100:.1f}%)")
        except: pass

        try:
            cpa = float(row['cost_conv']) if row['cost_conv'] else 0
            cpa_comp = float(row['cost_conv_compare_to']) if row['cost_conv_compare_to'] else 0
            if cpa_comp > 0 and cpa > (cpa_comp * 1.25):
                issues.append(f"CPA Rise ({(cpa - cpa_comp)/cpa_comp*100:.1f}%)")
        except: pass

        if issues:
            anomalies.append({
                "campaign_name": c_name,
                "campaign_type": c_type,
                "issues": issues
            })

    if not anomalies:
        return "No anomalies found."
    
    return json.dumps(anomalies, indent=2)

@tool
def call_pmax_agent(campaign_name: str, issues: List[str]) -> str:
    """
    Calls the PMax Sub-Agent to analyze a specific Performance Max campaign.
    Performs deep dive into Channels, Products, Locations, and Search Terms.
    """
    report = [f"### 🕵️ PMax Deep Dive: {campaign_name}"]
    report.append(f"**Trigger Issues**: {', '.join(issues)}\n")

    # A. Channel Analysis (Calculated Metrics)
    try:
        # Fetch raw metrics
        channels_data = query_db(
            """
            SELECT channels, status, cost, conversions, conv_value, clicks
            FROM channel 
            WHERE campaigns LIKE ? AND status = 'active'
            ORDER BY CAST(cost AS REAL) DESC
            """, 
            (f"%{campaign_name}%",)
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
        if zombies:
            report.append(f"❌ **Zombie Products** (High Cost, 0 Conv):")
            for z in zombies: report.append(f"- {z}")
            report.append("👉 **Action**: Exclude these products from the Listing Group immediately.")
        elif inefficient:
            report.append(f"⚠️ **Inefficient Products** (ROAS < 0.5):")
            for i in inefficient: report.append(f"- {i}")
            report.append("👉 **Action**: Consider excluding or optimizing titles/images.")
        else:
            report.append("✅ No major product issues found among top spenders.")
        report.append("")

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
def call_search_agent(campaign_name: str, issues: List[str]) -> str:
    """
    Calls the Search Sub-Agent to analyze a specific Search campaign.
    Uses the flash model to deeper analyze Search Terms, Match Types, Audiences.
    """
    data_context = []
    data_context.append(f"Campaign: {campaign_name}")
    data_context.append(f"Trigger Issues: {', '.join(issues)}")

    # A. Search Terms
    try:
        terms = query_db("SELECT search_term, match_type, cost, conversions, conv_value_cost FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 20", (campaign_name,))
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

class AgentService:
    def __init__(self):
        print(f"Initializing Main Agent with model={MAIN_MODEL_NAME}")
        self.llm = main_llm
        
        self.tools = [scan_campaigns_for_anomalies, call_pmax_agent, call_search_agent]
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
        if not isinstance(messages[0], SystemMessage):
            system_prompt = SystemMessage(content="""你是 Main Agent (任务调度器)。
你的职责是监控广告系列并将分析任务分派给子 Agent。

**核心指令:**
1. **扫描**: 收到用户请求先调用 `scan_campaigns_for_anomalies`。
2. **强制执行**: 只要扫描发现异常，**必须立即调用**对应的子 Agent (`call_pmax_agent` 或 `call_search_agent`)。**不要停止**，也不要只输出文本。必须生成 Tool Call。
3. **报告**: 汇总子 Agent 的分析结果。

**输出规则:**
- **不要** 输出 "正在分派"、"正在分析" 等过渡性语句。
- **引用规则**: 
  - 深度分析结果 **必须** 标注来源表 (如 "(数据来源: search_term 表)")。
  - 扫描结果列表 **不要** 标注来源。
- **所有输出必须使用中文。**
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

    async def chat_stream(self, message: str, messages: list):
        input_messages = []
        if messages:
            for msg in messages:
                if msg.role == 'user':
                    input_messages.append(HumanMessage(content=msg.content))
                elif msg.role == 'agent':
                    input_messages.append(AIMessage(content=msg.content))
        
        input_messages.append(HumanMessage(content=message))
        
        async for event in self.app.astream_events({"messages": input_messages}, version="v1"):
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
            'search_term', 'product', 'channel', 'asset', 
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
