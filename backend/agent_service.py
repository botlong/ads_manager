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
        "data_source": "campaign 表 - 按日期聚合的广告系列数据",
        "extraction_method": """
        1. 获取最近3天数据 (current_3d) 和前7天数据 (prev_7d)
        2. 计算均值: avg_roas = mean(prev_7d.roas), avg_cpa = mean(prev_7d.cpa)
        3. 计算增长率: growth = (current_3d.conv - prev_7d.conv) / prev_7d.conv
        """,
        "hard_rules": """
        🔴 ROAS崩盘: current_roas < avg_roas * 0.8 → 触发
        🔴 CPA飙升: current_cpa > avg_cpa * 1.25 → 触发
        🟡 增长停滞: growth <= 0 → 标记无增长风险
        ⛔ 风控保护: 大促期(双十一/黑五等)自动跳过
        """
    },
    "channel": {
        "title": "PMax Channel Analyst (PMax 渠道专家)",
        "focus": "PMax 渠道效率分析，识别低效流量吞噬。",
        "data_source": "channel 表 - PMax 渠道分布数据",
        "extraction_method": """
        函数: analyze_pmax_channel_efficiency(campaign_name, start_date, end_date)
        1. 按渠道聚合: SUM(cost), SUM(conv_value) GROUP BY channels
        2. 计算占比: spend_share = channel_cost / total_cost * 100
        3. 计算ROAS: channel_roas = conv_value / cost
        4. 获取账户均值: SELECT AVG(roas) FROM campaign
        """,
        "hard_rules": """
        🔴 Display占比>35% 且 ROAS<账户均值50% → "PMax吃低效流量"
        🟡 Video占比>30% 且 CPA>2.5x Target → "Video渠道高风险"
        ⚠️ ROAS Bottom 20% 渠道 → 标记为低效渠道
        """
    },
    "search_term": {
        "title": "Search Term Analyst (搜索词专家)",
        "focus": "搜索流量质量分析，识别低质匹配与垃圾词。",
        "data_source": "search_term 表 - 搜索词级别数据",
        "extraction_method": """
        函数: analyze_search_quality(campaign_name, start_date, end_date)
        1. 按匹配类型聚合: SUM(cost), SUM(conv) GROUP BY match_type
        2. 计算CPA: match_cpa = cost / conv
        3. 计算占比: spend_share = match_cost / total_cost * 100
        4. 关键词匹配: 检测垃圾模式 (free, repair, support, login 等)
        """,
        "hard_rules": """
        🔴 Broad占比>40% 且 Broad_CPA>1.5x Exact_CPA → "流量匹配质量下滑"
        🟡 检测到垃圾搜索词 (非购买意图) → 建议添加否定关键词
        """
    },
    "product": {
        "title": "Product SKU Analyst (产品/货架专家)",
        "focus": "商品结构分析，识别僵尸商品与预算垄断。",
        "data_source": "product 表 - 商品级别数据",
        "extraction_method": """
        函数: analyze_product_structure(campaign_name, start_date, end_date)
        1. 按商品聚合: SUM(cost), SUM(clicks) GROUP BY item_id
        2. 计算占比: top_share = top_product_cost / total_cost * 100
        3. 计算均价: avg_price = mean(all_products.price)
        4. 识别低价高消耗: price < avg_price * 0.5 AND cost > 20
        """,
        "hard_rules": """
        🔴 单品占预算>85% → "测试饥饿风险"
        🟡 Cost>$50 且 Clicks=0 → "僵尸商品"
        🟡 高消耗商品集中在低价SKU → "结构性毛利问题"
        """
    },
    "campaign": {
        "title": "Campaign Manager (广告系列管理专家)",
        "focus": "广告系列整体表现评估与时间对比。",
        "data_source": "campaign 表 - 系列级别数据",
        "extraction_method": """
        函数: calculate_time_comparison(table, campaign, metric, window_days)
        1. 获取当前N天: SUM(metric) WHERE date >= current_start
        2. 获取前N天: SUM(metric) WHERE date >= previous_start AND date < current_start
        3. 计算变化: change_pct = (current - previous) / previous * 100
        """,
        "hard_rules": """
        📈 change_pct > 10% → "上升"
        📉 change_pct < -10% → "下降"
        ➡️ -10% <= change_pct <= 10% → "持平"
        """
    },
    "location": {
        "title": "Location Expert (地域专家)",
        "focus": "地理位置效率分析，识别预算黑洞。",
        "data_source": "location_by_cities_all_campaign 表",
        "extraction_method": """
        直接查询: SELECT * WHERE cost > 50 AND conversions = 0
        """,
        "hard_rules": """
        ❌ Cost>$50 且 Conv=0 → "预算黑洞，建议排除"
        """
    },
    "audience": {
        "title": "Audience Analyst (受众专家)",
        "focus": "受众效率分析，识别低效信号。",
        "data_source": "audience 表 - 受众信号数据",
        "extraction_method": """
        直接查询: SELECT * ORDER BY cost DESC LIMIT 10
        """,
        "hard_rules": """
        ❌ Cost>$50 且 Conv=0 → "高消耗零转化受众，建议排除"
        """
    },
    # Aliases for frontend compatibility (大写开头的别名)
    "Campaigns": {
        "title": "Campaign Manager (广告系列管理专家)",
        "focus": "广告系列整体表现评估与时间对比。",
        "data_source": "campaign 表 - 系列级别数据",
        "extraction_method": """
        函数: calculate_time_comparison(table, campaign, metric, window_days)
        1. 获取当前N天: SUM(metric) WHERE date >= current_start
        2. 获取前N天: SUM(metric) WHERE date >= previous_start AND date < current_start
        3. 计算变化: change_pct = (current - previous) / previous * 100
        """,
        "hard_rules": """
        📈 change_pct > 10% → "上升"
        📉 change_pct < -10% → "下降"
        ➡️ -10% <= change_pct <= 10% → "持平"
        """
    },
    "Products": {
        "title": "Product SKU Analyst (产品/货架专家)",
        "focus": "商品结构分析，识别僵尸商品与预算垄断。",
        "data_source": "product 表 - 商品级别数据",
        "extraction_method": """
        函数: analyze_product_structure(campaign_name, start_date, end_date)
        1. 按商品聚合: SUM(cost), SUM(clicks) GROUP BY item_id
        2. 计算占比: top_share = top_product_cost / total_cost * 100
        3. 识别僵尸商品: cost > 50 AND clicks = 0
        """,
        "hard_rules": """
        🔴 单品占预算>85% → "测试饥饿风险"
        🟡 Cost>$50 且 Clicks=0 → "僵尸商品"
        🟡 高消耗商品集中在低价SKU → "结构性毛利问题"
        """
    },
    "asset": {
        "title": "Creative Asset Analyst (素材创意专家)",
        "focus": "创意素材效果评估。",
        "data_source": "asset 表 - 素材级别数据",
        "extraction_method": """
        直接查询: SELECT * ORDER BY cost DESC
        """,
        "hard_rules": """
        🟡 CTR连续7天下降>20% → "素材疲劳"
        ❌ Cost>$50 且 Conv=0 → "低效素材"
        """
    },
    "age": {
        "title": "Age Demographics Analyst (年龄分层专家)",
        "focus": "年龄维度效率分析。",
        "data_source": "age 表 - 年龄段维度数据",
        "extraction_method": """
        直接查询: SELECT age, SUM(cost), SUM(conversions) GROUP BY age
        """,
        "hard_rules": """
        ❌ Cost>2x CPA 且 Conv=0 且 14天+ → "建议排除"
        🟡 Clicks<25 → "数据不足，继续观察"
        """
    },
    "gender": {
        "title": "Gender Demographics Analyst (性别分层专家)",
        "focus": "性别维度效率分析。",
        "data_source": "gender 表 - 性别维度数据",
        "extraction_method": """
        直接查询: SELECT gender, SUM(cost), SUM(conversions) GROUP BY gender
        """,
        "hard_rules": """
        ❌ Cost>$100 且 Conv=0 → "建议降低出价"
        🟡 Clicks<100 → "数据不足，继续观察"
        """
    },
    "ad_schedule": {
        "title": "Time & Schedule Analyst (分时专家)",
        "focus": "分时段效率分析。",
        "data_source": "ad_schedule 表 - 分时段数据",
        "extraction_method": """
        直接查询: SELECT day_and_time, SUM(cost), SUM(conversions) GROUP BY day_and_time
        """,
        "hard_rules": """
        ❌ Cost>3x CPA 且 Conv=0 → "建议排除该时段"
        🟡 00:00-05:00 → "注意延迟归因"
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

# =============================================================================
# HARD-CODED EXPERT ANALYSIS FUNCTIONS (硬规则分析)
# These functions pre-calculate verdicts using Python instead of LLM interpretation
# =============================================================================

def analyze_pmax_channel_efficiency(campaign_name: str, start_date: str = None, end_date: str = None) -> Dict:
    """
    硬规则判定 PMax 渠道效率:
    - Display Spend 占比 >35% 且 ROAS < 全账户均值50% → "PMax 吃低效流量"
    - Video Spend >30% 且 CPA > 2.5x Target → "高风险"
    - 返回预计算的判定结论
    """
    verdicts = []
    evidence = {}
    recommendations = []
    
    # Date filter
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = "AND date >= ? AND date <= ?"
        params = [start_date, end_date]
    
    # Get channel data for this campaign
    channel_query = f"""
        SELECT channels, SUM(cost) as total_cost, SUM(conversions) as total_conv,
               SUM(conv_value) as total_value
        FROM channel 
        WHERE campaigns LIKE ? {date_filter}
        GROUP BY channels
    """
    channel_data = query_db(channel_query, (f"%{campaign_name}%", *params))
    
    if not channel_data:
        return {"verdicts": [], "evidence": {"note": "无渠道数据"}, "recommendations": []}
    
    # Calculate totals
    total_spend = sum(c.get('total_cost', 0) or 0 for c in channel_data)
    account_roas = 0
    
    # Get account average ROAS
    account_stats = query_db(f"SELECT AVG(roas) as avg_roas FROM campaign WHERE date >= ? AND date <= ?", 
                             (start_date or '2020-01-01', end_date or '2099-12-31'))
    if account_stats and account_stats[0].get('avg_roas'):
        account_roas = account_stats[0]['avg_roas']
    
    evidence['account_avg_roas'] = round(account_roas, 2)
    evidence['total_spend'] = round(total_spend, 2)
    evidence['channels'] = []
    
    for channel in channel_data:
        ch_name = channel.get('channels', 'Unknown')
        ch_cost = channel.get('total_cost', 0) or 0
        ch_conv = channel.get('total_conv', 0) or 0
        ch_value = channel.get('total_value', 0) or 0
        
        spend_share = (ch_cost / total_spend * 100) if total_spend > 0 else 0
        ch_roas = (ch_value / ch_cost) if ch_cost > 0 else 0
        ch_cpa = (ch_cost / ch_conv) if ch_conv > 0 else 0
        
        evidence['channels'].append({
            'channel': ch_name,
            'spend': round(ch_cost, 2),
            'spend_share_pct': round(spend_share, 1),
            'roas': round(ch_roas, 2),
            'conversions': ch_conv
        })
        
        # Rule 1: Display Spend >35% AND ROAS < Account Avg 50%
        if 'display' in ch_name.lower():
            if spend_share > 35 and ch_roas < (account_roas * 0.5):
                verdicts.append({
                    'verdict': '🔴 判定: PMax 吃低效流量 (Display)',
                    'severity': 'CRITICAL',
                    'rule': f'Display Spend占比 {spend_share:.1f}% > 35% 且 ROAS {ch_roas:.2f} < 账户均值50% ({account_roas*0.5:.2f})'
                })
                recommendations.append('建议: 在 PMax 中排除 Display 版位或降低素材展示频率')
        
        # Rule 2: Video Spend >30% AND CPA > 2.5x Target
        if 'video' in ch_name.lower():
            target_cpa = account_roas * 10 if account_roas > 0 else 20  # Estimate target CPA
            if spend_share > 30 and ch_cpa > (target_cpa * 2.5):
                verdicts.append({
                    'verdict': '🟡 判定: Video 渠道高风险',
                    'severity': 'WARNING',
                    'rule': f'Video Spend占比 {spend_share:.1f}% > 30% 且 CPA ${ch_cpa:.2f} > 2.5x Target'
                })
                recommendations.append('建议: 检查 Video 素材质量，考虑降低 Video 预算分配')
    
    # Rule 3: Mark ROAS Bottom 20% channels
    if len(evidence['channels']) > 1:
        sorted_channels = sorted(evidence['channels'], key=lambda x: x['roas'])
        bottom_20_count = max(1, len(sorted_channels) // 5)
        bottom_channels = sorted_channels[:bottom_20_count]
        for bc in bottom_channels:
            if bc['spend'] > 10:  # Only flag if meaningful spend
                verdicts.append({
                    'verdict': f"⚠️ ROAS Bottom 20%: {bc['channel']}",
                    'severity': 'INFO',
                    'rule': f"ROAS {bc['roas']:.2f} 在所有渠道中排名最低"
                })
    
    if not verdicts:
        verdicts.append({'verdict': '✅ 渠道效率正常', 'severity': 'OK', 'rule': '未触发任何效率异常规则'})
    
    return {'verdicts': verdicts, 'evidence': evidence, 'recommendations': recommendations}


def analyze_search_quality(campaign_name: str, start_date: str = None, end_date: str = None) -> Dict:
    """
    硬规则判定 Search 流量质量:
    - Broad Match 占比 >40% 且 Broad CPA > 1.5x Exact CPA → "流量匹配质量下滑"
    - 检测垃圾搜索词 (free, repair, support, login 等)
    - 返回预计算的判定结论
    """
    verdicts = []
    evidence = {}
    recommendations = []
    junk_terms = []
    
    # Junk term patterns
    JUNK_PATTERNS = ['free', 'repair', 'support', 'login', 'manual', 'driver', 'download', 
                     'firmware', 'reset', 'unlock', 'crack', 'hack', 'cheap', 'used', 'refurbished',
                     'whatsapp', 'phone number', 'customer service', 'troubleshoot']
    
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = "AND date >= ? AND date <= ?"
        params = [start_date, end_date]
    
    # Get search term data
    search_query = f"""
        SELECT search_term, match_type, SUM(cost) as total_cost, SUM(conversions) as total_conv,
               SUM(impressions) as total_clicks
        FROM search_term 
        WHERE campaign LIKE ? {date_filter}
        GROUP BY search_term, match_type
    """
    search_data = query_db(search_query, (f"%{campaign_name}%", *params))
    
    if not search_data:
        return {"verdicts": [], "evidence": {"note": "无搜索词数据"}, "junk_terms": [], "recommendations": []}
    
    # Calculate match type stats
    match_stats = {}
    for row in search_data:
        mt = (row.get('match_type') or 'Unknown').lower()
        if mt not in match_stats:
            match_stats[mt] = {'cost': 0, 'conv': 0, 'clicks': 0}
        match_stats[mt]['cost'] += row.get('total_cost', 0) or 0
        match_stats[mt]['conv'] += row.get('total_conv', 0) or 0
        match_stats[mt]['clicks'] += row.get('total_clicks', 0) or 0
    
    total_cost = sum(m['cost'] for m in match_stats.values())
    
    # Calculate CPA per match type
    for mt in match_stats:
        match_stats[mt]['cpa'] = (match_stats[mt]['cost'] / match_stats[mt]['conv']) if match_stats[mt]['conv'] > 0 else 0
        match_stats[mt]['spend_share'] = (match_stats[mt]['cost'] / total_cost * 100) if total_cost > 0 else 0
    
    evidence['match_type_stats'] = {k: {
        'spend': round(v['cost'], 2),
        'spend_share_pct': round(v['spend_share'], 1),
        'conversions': v['conv'],
        'cpa': round(v['cpa'], 2)
    } for k, v in match_stats.items()}
    
    # Rule 1: Broad Match >40% spend AND Broad CPA > 1.5x Exact CPA
    broad_stats = match_stats.get('broad', match_stats.get('broad match', {}))
    exact_stats = match_stats.get('exact', match_stats.get('exact match', {}))
    
    if broad_stats and exact_stats:
        broad_share = broad_stats.get('spend_share', 0)
        broad_cpa = broad_stats.get('cpa', 0)
        exact_cpa = exact_stats.get('cpa', 0)
        
        if broad_share > 40 and exact_cpa > 0 and broad_cpa > (exact_cpa * 1.5):
            verdicts.append({
                'verdict': '🔴 判定: 流量匹配质量下滑',
                'severity': 'CRITICAL',
                'rule': f'Broad Match占比 {broad_share:.1f}% > 40% 且 Broad CPA ${broad_cpa:.2f} > 1.5x Exact CPA ${exact_cpa:.2f}'
            })
            recommendations.append('建议: 将高消耗 Broad Match 关键词转为 Phrase 或 Exact Match')
            recommendations.append('建议: 检查 Broad 带来的低质搜索词并添加为否定关键词')
    
    # Rule 2: Detect junk search terms
    for row in search_data:
        term = (row.get('search_term') or '').lower()
        cost = row.get('total_cost', 0) or 0
        conv = row.get('total_conv', 0) or 0
        
        for pattern in JUNK_PATTERNS:
            if pattern in term and cost > 5:  # Only flag if spent > $5
                junk_terms.append({
                    'term': row.get('search_term'),
                    'pattern_matched': pattern,
                    'cost': round(cost, 2),
                    'conversions': conv
                })
                break
    
    if junk_terms:
        total_junk_cost = sum(j['cost'] for j in junk_terms)
        verdicts.append({
            'verdict': f'🟡 检测到 {len(junk_terms)} 个垃圾搜索词',
            'severity': 'WARNING',
            'rule': f'匹配到非购买意图关键词，浪费预算 ${total_junk_cost:.2f}'
        })
        recommendations.append(f'建议: 将以下搜索词添加为否定关键词: {", ".join([j["term"] for j in junk_terms[:5]])}')
    
    evidence['junk_terms_count'] = len(junk_terms)
    evidence['junk_terms_cost'] = round(sum(j['cost'] for j in junk_terms), 2)
    
    if not verdicts:
        verdicts.append({'verdict': '✅ 搜索流量质量正常', 'severity': 'OK', 'rule': '未触发任何流量质量异常规则'})
    
    return {'verdicts': verdicts, 'evidence': evidence, 'junk_terms': junk_terms[:10], 'recommendations': recommendations}


def analyze_product_structure(campaign_name: str, start_date: str = None, end_date: str = None) -> Dict:
    """
    硬规则判定商品结构性问题:
    - 低ROAS商品集中在低价SKU → "结构性毛利问题"
    - 单品预算占比 >85% → "测试饥饿风险"
    - 僵尸商品: Cost > $50 且 0 Conv
    - 返回预计算的判定结论
    """
    verdicts = []
    evidence = {}
    recommendations = []
    zombies = []
    
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = "AND date >= ? AND date <= ?"
        params = [start_date, end_date]
    
    # Get product data
    product_query = f"""
        SELECT title, item_id, price, SUM(cost) as total_cost, SUM(clicks) as total_clicks,
               SUM(impr) as total_impr
        FROM product 
        WHERE 1=1 {date_filter}
        GROUP BY item_id
        ORDER BY total_cost DESC
    """
    product_data = query_db(product_query, tuple(params))
    
    if not product_data:
        return {"verdicts": [], "evidence": {"note": "无商品数据"}, "zombies": [], "recommendations": []}
    
    total_spend = sum(p.get('total_cost', 0) or 0 for p in product_data)
    avg_price = sum(p.get('price', 0) or 0 for p in product_data) / len(product_data) if product_data else 0
    
    evidence['total_products'] = len(product_data)
    evidence['total_spend'] = round(total_spend, 2)
    evidence['avg_price'] = round(avg_price, 2)
    
    # Rule 1: Budget Hegemony - Single product >85% spend
    if product_data and total_spend > 0:
        top_product = product_data[0]
        top_spend = top_product.get('total_cost', 0) or 0
        top_share = (top_spend / total_spend * 100)
        
        if top_share > 85:
            verdicts.append({
                'verdict': f'🔴 判定: 测试饥饿风险',
                'severity': 'CRITICAL',
                'rule': f'单品 "{top_product.get("title", "")[:30]}..." 占预算 {top_share:.1f}% > 85%'
            })
            recommendations.append('建议: 均衡分配预算，让其他商品有机会获得展示')
            evidence['hegemony_product'] = top_product.get('title', '')[:50]
            evidence['hegemony_share'] = round(top_share, 1)
    
    # Rule 2: Zombie products - Cost > $50 AND 0 clicks (no conversion data available, use clicks as proxy)
    for product in product_data:
        cost = product.get('total_cost', 0) or 0
        clicks = product.get('total_clicks', 0) or 0
        
        if cost > 50 and clicks == 0:
            zombies.append({
                'item_id': product.get('item_id'),
                'title': (product.get('title') or '')[:40],
                'cost': round(cost, 2),
                'clicks': clicks
            })
    
    if zombies:
        total_zombie_cost = sum(z['cost'] for z in zombies)
        verdicts.append({
            'verdict': f'🟡 检测到 {len(zombies)} 个僵尸商品',
            'severity': 'WARNING',
            'rule': f'消耗 > $50 但 0 点击，浪费预算 ${total_zombie_cost:.2f}'
        })
        recommendations.append('建议: 在 Listing Group 中排除僵尸商品或暂停投放')
    
    evidence['zombie_count'] = len(zombies)
    evidence['zombie_total_cost'] = round(sum(z['cost'] for z in zombies), 2)
    
    # Rule 3: Structural margin issue - Low ROAS products are low-price SKUs
    # Sort by cost (high spenders) and check if low-price
    high_spend_products = [p for p in product_data if (p.get('total_cost', 0) or 0) > 20]
    low_price_high_spend = [p for p in high_spend_products if (p.get('price', 0) or 0) < avg_price * 0.5]
    
    if len(low_price_high_spend) > len(high_spend_products) * 0.5 and len(high_spend_products) > 3:
        verdicts.append({
            'verdict': '🟡 判定: 结构性毛利问题',
            'severity': 'WARNING',
            'rule': f'高消耗商品中 {len(low_price_high_spend)}/{len(high_spend_products)} 个是低价SKU (< 均价50%)'
        })
        recommendations.append('建议: 检查低价SKU的毛利率，考虑调整商品优先级或排除低毛利商品')
        evidence['low_price_high_spend_ratio'] = f"{len(low_price_high_spend)}/{len(high_spend_products)}"
    
    if not verdicts:
        verdicts.append({'verdict': '✅ 商品结构正常', 'severity': 'OK', 'rule': '未触发任何结构性问题规则'})
    
    return {'verdicts': verdicts, 'evidence': evidence, 'zombies': zombies[:10], 'recommendations': recommendations}


def calculate_time_comparison(table_name: str, campaign_name: str, metric: str, window_days: int = 7) -> Dict:
    """
    计算时间窗口对比: 当前N天 vs 前N天
    返回: {current: x, previous: y, change_pct: z, verdict: "..."}
    """
    from datetime import datetime, timedelta
    
    today = datetime.now().date()
    current_end = today
    current_start = today - timedelta(days=window_days)
    previous_end = current_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=window_days)
    
    # Query current period
    current_query = f"""
        SELECT SUM({metric}) as total FROM {table_name}
        WHERE campaign LIKE ? AND date >= ? AND date <= ?
    """
    current_val = query_value(current_query, (f"%{campaign_name}%", str(current_start), str(current_end))) or 0
    
    # Query previous period
    previous_val = query_value(current_query, (f"%{campaign_name}%", str(previous_start), str(previous_end))) or 0
    
    # Calculate change
    if previous_val > 0:
        change_pct = ((current_val - previous_val) / previous_val) * 100
    else:
        change_pct = 100 if current_val > 0 else 0
    
    # Generate verdict
    verdict = "持平"
    if change_pct > 10:
        verdict = f"📈 上升 {change_pct:.1f}%"
    elif change_pct < -10:
        verdict = f"📉 下降 {abs(change_pct):.1f}%"
    
    return {
        'metric': metric,
        'window': f'{window_days}d vs 前{window_days}d',
        'current': round(current_val, 2),
        'previous': round(previous_val, 2),
        'change_pct': round(change_pct, 1),
        'verdict': verdict
    }

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
    # 表名大小写规范化 - 处理AI可能使用的大写表名
    TABLE_NAME_MAP = {
        'Products': 'product',
        'Campaigns': 'campaign', 
        'Channel': 'channel',
        'Search_term': 'search_term',
        'Age': 'age',
        'Gender': 'gender',
        'Location': 'location',
        'Audience': 'audience',
        'Asset': 'asset',
        'Ad_schedule': 'ad_schedule',
    }
    table_name = TABLE_NAME_MAP.get(table_name, table_name)
    
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
    你是专业的 Google Ads 优化专家，专注于: {expert.get('title', f'{table_name} 分析专家')}。
    
    **分析目标**: {expert.get('focus', '数据效率分析')}
    **指标说明**: {expert.get('metrics_desc', '请根据数据列自行判断指标含义')}
    
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
    
    print(f"🔍 Scan found {len(anomalies)} anomaly campaigns for date: {target_date}")
    for a in anomalies:
        print(f"   - {a.get('campaign', 'Unknown')}: {a.get('reason', 'No reason')}")
    
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

    # 🚨 添加强制性摘要头部 - 确保AI无法忽略任何异常
    report = []
    report.append(f"## 🚨 全账户异常扫描结果")
    report.append(f"**扫描发现 {len(anomalies)} 个异常广告系列，以下每个都必须在最终报告中涵盖：**\n")
    for i, a in enumerate(anomalies, 1):
        report.append(f"{i}. **{a.get('campaign', 'Unknown')}** - {a.get('reason', 'Efficiency Issue')}")
    report.append("\n---\n")
    report.append("## 各系列详细诊断\n")
    for a in anomalies:
        campaign_name = a.get('campaign', 'Unknown')
        campaign_type = a.get('campaign_type', 'Unknown')
        
        # 收集相关数据供 LLM 分析
        related_data = {}
        
        # 搜索词数据
        st_data = query_db("SELECT * FROM search_term WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 15", (campaign_name,))
        if st_data:
            related_data['search_term'] = st_data
        
        # 渠道数据 (PMax)
        ch_data = query_db("SELECT * FROM channel WHERE campaigns LIKE ? ORDER BY CAST(cost AS REAL) DESC LIMIT 15", (f"%{campaign_name}%",))
        if ch_data:
            related_data['channel'] = ch_data
        
        # 商品数据
        pr_data = query_db("SELECT * FROM product ORDER BY CAST(cost AS REAL) DESC LIMIT 15")
        if pr_data:
            related_data['product'] = pr_data
        
        # 地域数据
        geo_data = query_db("SELECT * FROM location WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 15", (campaign_name,))
        if geo_data:
            related_data['geo'] = geo_data
        
        # 年龄数据
        age_data = query_db("SELECT * FROM age WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        if age_data:
            related_data['age'] = age_data
        
        # 时段数据
        schedule_data = query_db("SELECT * FROM ad_schedule WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        if schedule_data:
            related_data['ad_schedule'] = schedule_data
        
        # 调试日志
        print(f"📊 Collected data for {campaign_name}:")
        for key, val in related_data.items():
            print(f"   - {key}: {len(val)} rows")

        # 纯 LLM 分析 - 加强详细输出要求
        prompt = f"""
        你是 Google Ads 全账户巡检专家。请提供**非常详细**的分析报告。
        
        ---
        ## ⚠️ 必须遵守的分析规则
        
        {analysis_rules}
        
        ---
        ## 触发异常的广告系列
        
        **系列名称**: {campaign_name}
        **系列类型**: {campaign_type}
        **异常数据**: {json.dumps(a, ensure_ascii=False)}
        
        ## 相关维度数据 (请基于以下数据做详细分析)
        
        {json.dumps(related_data, ensure_ascii=False, indent=2)[:12000]}
        
        ---
        ## 🚨 输出要求 (必须遵守)
        
        你必须输出详细的诊断报告，格式如下：
        
        1. **效率概览**: 用具体数字说明ROAS/CPA的变化幅度
        2. **搜索词分析**: 如果有search_term数据，列出消耗Top 5的词及其CTR/转化
        3. **渠道分析**: 如果有channel数据，列出各渠道消耗占比
        4. **人群画像**: 如果有age数据，列出年龄段分布
        5. **时段分析**: 如果有schedule数据，列出高消耗时段
        6. **根本原因**: 综合判断效率下降的根本原因
        7. **行动建议**: 给出3-5条具体可执行的优化建议
        
        **重要**: 即使数据不完整，也要基于现有数据给出尽可能详细的分析。不要给出"无数据"的简单回复。
        """
        
        llm_analysis = sub_llm.invoke(prompt).content
        report.append(f"### ⚠️ {campaign_name}\n\n{llm_analysis}\n")

    return "\n".join(report)


def call_pmax_agent(campaign_name: str, issues: List[str], start_date: str = None, end_date: str = None) -> str:
    """
    Calls the PMax Sub-Agent to analyze a specific Performance Max campaign within a date range.
    Uses HARD-CODED expert rules for pre-calculated verdicts.
    """
    report = [f"### 🕵️ PMax Deep Dive: {campaign_name}"]
    report.append(f"**Trigger Issues**: {', '.join(issues)}\n")

    # =========================================================================
    # A. Channel Analysis - USING HARD-CODED EXPERT RULES
    # =========================================================================
    try:
        channel_analysis = analyze_pmax_channel_efficiency(campaign_name, start_date, end_date)
        
        report.append("#### 📡 A. Channel Efficiency Analysis (硬规则判定)")
        
        # Display verdicts
        for v in channel_analysis.get('verdicts', []):
            verdict = v.get('verdict', '')
            rule = v.get('rule', '')
            report.append(f"**{verdict}**")
            if rule:
                report.append(f"   - 规则: {rule}")
        
        # Display evidence
        evidence = channel_analysis.get('evidence', {})
        if evidence.get('channels'):
            report.append("\n**渠道数据:**")
            for ch in evidence['channels']:
                report.append(f"- **{ch['channel']}**: Spend ${ch['spend']:.2f} ({ch['spend_share_pct']:.1f}%) | ROAS {ch['roas']:.2f} | Conv {ch['conversions']}")
        
        # Display recommendations
        for rec in channel_analysis.get('recommendations', []):
            report.append(f"👉 {rec}")
        
        report.append("")
    except Exception as e:
        report.append(f"Error in Channel Analysis: {e}")

    # =========================================================================
    # B. Product Structure Analysis - USING HARD-CODED EXPERT RULES
    # =========================================================================
    try:
        product_analysis = analyze_product_structure(campaign_name, start_date, end_date)
        
        report.append("#### 📦 B. Product Structure Analysis (硬规则判定)")
        
        # Display verdicts
        for v in product_analysis.get('verdicts', []):
            verdict = v.get('verdict', '')
            rule = v.get('rule', '')
            report.append(f"**{verdict}**")
            if rule:
                report.append(f"   - 规则: {rule}")
        
        # Display zombies
        zombies = product_analysis.get('zombies', [])
        if zombies:
            report.append("\n**僵尸商品列表:**")
            for z in zombies[:5]:
                report.append(f"- {z['title']} (ID: {z['item_id']}) - Cost ${z['cost']:.2f}, Clicks {z['clicks']}")
        
        # Display recommendations
        for rec in product_analysis.get('recommendations', []):
            report.append(f"👉 {rec}")
        
        report.append("")
    except Exception as e:
        report.append(f"Error in Product Analysis: {e}")

    # =========================================================================
    # C. Location Analysis (Keep existing logic - straightforward)
    # =========================================================================
    try:
        locs = query_db("SELECT location_name, cost, conversions FROM location WHERE campaign = ? AND CAST(cost AS REAL) > 50 AND CAST(conversions AS REAL) = 0 ORDER BY CAST(cost AS REAL) DESC LIMIT 3", (campaign_name,))
        report.append("#### 🌍 C. Location Analysis")
        if locs:
            report.append("❌ **Money Wasting Locations**:")
            for l in locs:
                report.append(f"- **{l.get('location_name', l.get('location', 'Unknown'))}**: Cost ${l.get('cost')}, 0 Conv")
            report.append("👉 **Action**: Exclude these locations in Campaign Settings.")
        else:
            report.append("✅ No high-spend zero-conversion locations found.")
        report.append("")
    except Exception as e:
        report.append(f"Error in Location Analysis: {e}")

    # =========================================================================
    # D. Search Term Analysis - USING HARD-CODED EXPERT RULES
    # =========================================================================
    try:
        search_analysis = analyze_search_quality(campaign_name, start_date, end_date)
        
        report.append("#### 🔍 D. Search Term Quality Analysis (硬规则判定)")
        
        # Display verdicts
        for v in search_analysis.get('verdicts', []):
            verdict = v.get('verdict', '')
            rule = v.get('rule', '')
            report.append(f"**{verdict}**")
            if rule:
                report.append(f"   - 规则: {rule}")
        
        # Display junk terms
        junk_terms = search_analysis.get('junk_terms', [])
        if junk_terms:
            report.append("\n**垃圾搜索词列表:**")
            for jt in junk_terms[:5]:
                report.append(f"- '{jt['term']}' (Cost ${jt['cost']:.2f}) - 匹配模式: {jt['pattern_matched']}")
        
        # Display recommendations
        for rec in search_analysis.get('recommendations', []):
            report.append(f"👉 {rec}")
            
    except Exception as e:
        report.append(f"Error in Search Term Analysis: {e}")

    # =========================================================================
    # E. Time Comparison (7d vs 前7d)
    # =========================================================================
    try:
        report.append("\n#### 📊 E. Time Window Comparison (时间窗口对比)")
        
        for metric in ['cost', 'conversions', 'conv_value']:
            comparison = calculate_time_comparison('campaign', campaign_name, metric, 7)
            report.append(f"- **{metric.upper()}**: {comparison['current']} vs {comparison['previous']} ({comparison['verdict']})")
        
    except Exception as e:
        report.append(f"Error in Time Comparison: {e}")

    return "\n".join(report)


def call_search_agent(campaign_name: str, issues: List[str], start_date: str = None, end_date: str = None) -> str:
    """
    Calls the Search Sub-Agent to analyze a specific Search campaign within a date range.
    Uses HARD-CODED expert rules for pre-calculated verdicts.
    """
    report = [f"### 🔍 Search Campaign Deep Dive: {campaign_name}"]
    report.append(f"**Trigger Issues**: {', '.join(issues)}\n")

    # =========================================================================
    # A. Search Quality Analysis - USING HARD-CODED EXPERT RULES
    # =========================================================================
    try:
        search_analysis = analyze_search_quality(campaign_name, start_date, end_date)
        
        report.append("#### 📊 A. Search Quality Analysis (硬规则判定)")
        
        # Display verdicts
        for v in search_analysis.get('verdicts', []):
            verdict = v.get('verdict', '')
            rule = v.get('rule', '')
            report.append(f"**{verdict}**")
            if rule:
                report.append(f"   - 规则: {rule}")
        
        # Display match type stats
        evidence = search_analysis.get('evidence', {})
        match_stats = evidence.get('match_type_stats', {})
        if match_stats:
            report.append("\n**匹配类型效能:**")
            for mt, stats in match_stats.items():
                report.append(f"- **{mt.upper()}**: Spend ${stats['spend']:.2f} ({stats['spend_share_pct']:.1f}%) | CPA ${stats['cpa']:.2f} | Conv {stats['conversions']}")
        
        # Display junk terms
        junk_terms = search_analysis.get('junk_terms', [])
        if junk_terms:
            report.append("\n**垃圾搜索词列表:**")
            for jt in junk_terms[:5]:
                report.append(f"- '{jt['term']}' (Cost ${jt['cost']:.2f}) - 匹配模式: {jt['pattern_matched']}")
        
        # Display recommendations
        for rec in search_analysis.get('recommendations', []):
            report.append(f"👉 {rec}")
        
        report.append("")
    except Exception as e:
        report.append(f"Error in Search Quality Analysis: {e}")

    # =========================================================================
    # B. Audience Analysis (optional - table may not exist)
    # =========================================================================
    try:
        # Try to query audience data - table may not exist in all databases
        audiences = query_db("SELECT * FROM age WHERE campaign = ? ORDER BY CAST(cost AS REAL) DESC LIMIT 10", (campaign_name,))
        
        report.append("#### 👥 B. Age Demographics Analysis")
        
        if audiences:
            waste_audiences = []
            for a in audiences:
                cost = float(a.get('cost', 0) or 0)
                conv = float(a.get('conversions', 0) or 0)
                if cost > 50 and conv == 0:
                    waste_audiences.append({
                        'segment': a.get('age', 'Unknown'),
                        'cost': cost
                    })
            
            if waste_audiences:
                report.append("❌ **高消耗零转化年龄段:**")
                for wa in waste_audiences[:5]:
                    report.append(f"- {wa['segment']} - Cost ${wa['cost']:.2f}, 0 Conv")
                report.append("👉 **建议**: 考虑降低这些年龄段的出价")
            else:
                report.append("✅ 未发现高消耗零转化的年龄段")
        else:
            report.append("ℹ️ 无年龄段数据")
        
        report.append("")
    except Exception as e:
        report.append(f"ℹ️ 年龄段分析跳过: 数据不可用")

    # =========================================================================
    # C. Time Comparison (7d vs 前7d)
    # =========================================================================
    try:
        report.append("#### 📈 C. Time Window Comparison (时间窗口对比)")
        
        for metric in ['cost', 'conversions']:
            comparison = calculate_time_comparison('campaign', campaign_name, metric, 7)
            report.append(f"- **{metric.upper()}**: {comparison['current']} vs {comparison['previous']} ({comparison['verdict']})")
        
    except Exception as e:
        report.append(f"Error in Time Comparison: {e}")

    return "\n".join(report)


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
    
    Logic aligned with Campaign Monitor but adapted for Product table columns:
    - Product table has: date, title, item_id, clicks, impr, ctr, avg_cpc, cost
    - Product table LACKS: conversions, ROAS
    
    So we use:
    - CTR instead of ROAS (efficiency metric)
    - CPC instead of CPA (cost efficiency metric)  
    - Clicks instead of Conversions (volume metric)
    
    Detection criteria:
    1. Efficiency Check (3 consecutive days):
       - CTR < 80% of 7-day avg
       - OR CPC > 125% of 7-day avg
    2. Growth Check:
       - No click growth (Current 3 days vs Previous 7 days)
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
        
        # 2. Fetch raw data (Last 45 days relative to target_date)
        query = """
            SELECT date, title, item_id, cost, clicks, impr, ctr, avg_cpc
            FROM product 
            WHERE date <= ? AND date >= date(?, '-45 days')
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
        df['avg_cpc'] = pd.to_numeric(df['avg_cpc'].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)

        anomalies = []
        
        # Group by product (item_id)
        for item_id, group in df.groupby('item_id'):
            group = group.sort_values('date')
            if len(group) < 10:
                continue

            title = group['title'].iloc[-1] if 'title' in group.columns else 'Unknown Product'
            last_date = target_dt
            
            # Check 3 days: T, T-1, T-2
            check_dates = [last_date - pd.Timedelta(days=i) for i in range(3)] 
            
            # Check Condition A: Efficiency for EACH of the last 3 days
            # Using CTR instead of ROAS, CPC instead of CPA
            is_efficiency_bad = True
            
            for d in check_dates:
                # Specific day row
                day_row = group[group['date'] == d]
                if day_row.empty:
                    is_efficiency_bad = False; break
                
                current_ctr = day_row['ctr'].values[0]
                current_cpc = day_row['avg_cpc'].values[0]

                # History: 7 days prior to 'd' -> [d-7, d-1]
                start_hist = d - pd.Timedelta(days=7)
                end_hist = d - pd.Timedelta(days=1)
                
                hist_rows = group[(group['date'] >= start_hist) & (group['date'] <= end_hist)]
                if hist_rows.empty:
                    is_efficiency_bad = False; break
                    
                avg_ctr = hist_rows['ctr'].mean()
                avg_cpc = hist_rows['avg_cpc'].mean()
                
                # Criteria (same thresholds as Campaign: 80% / 125%)
                ctr_bad = (avg_ctr > 0) and (current_ctr < avg_ctr * 0.8)
                cpc_bad = (avg_cpc > 0) and (current_cpc > avg_cpc * 1.25)
                
                if not (ctr_bad or cpc_bad):
                    is_efficiency_bad = False
                    break
            
            if not is_efficiency_bad:
                continue

            # Check Condition B: No Growth (using clicks instead of conversions)
            # Current Period: [T-2, T]
            # Week-over-week Previous Period: [T-9, T-7]
            current_start = last_date - pd.Timedelta(days=2)
            prev_end = last_date - pd.Timedelta(days=7)
            prev_start = prev_end - pd.Timedelta(days=2)
            
            current_clicks = group[(group['date'] >= current_start) & (group['date'] <= last_date)]['clicks'].sum()
            prev_clicks = group[(group['date'] >= prev_start) & (group['date'] <= prev_end)]['clicks'].sum()
            
            growth = 0
            if prev_clicks > 0:
                growth = (current_clicks - prev_clicks) / prev_clicks
            
            is_growth_bad = False
            if prev_clicks > 0:
                 if growth <= 0: is_growth_bad = True
            else:
                 if current_clicks == 0: is_growth_bad = True
            
            if is_growth_bad:
                # Calculate summary stats for display (3d vs prev 7d)
                curr_3d_mask = (group['date'] >= current_start) & (group['date'] <= last_date)
                prev_7d_mask = (group['date'] >= last_date - pd.Timedelta(days=9)) & (group['date'] <= last_date - pd.Timedelta(days=3))
                
                curr_cost = group[curr_3d_mask]['cost'].sum()
                prev_cost = group[prev_7d_mask]['cost'].sum()
                
                curr_clicks_sum = group[curr_3d_mask]['clicks'].sum()
                prev_clicks_sum = group[prev_7d_mask]['clicks'].sum()
                
                # Calculate average CTR/CPC for the periods to show trend
                curr_avg_ctr = group[curr_3d_mask]['ctr'].mean()
                prev_avg_ctr = group[prev_7d_mask]['ctr'].mean()
                curr_avg_cpc = group[curr_3d_mask]['avg_cpc'].mean()
                prev_avg_cpc = group[prev_7d_mask]['avg_cpc'].mean()

                # Determine specific efficiency reason
                efficiency_details = []

                if prev_avg_ctr > 0 and curr_avg_ctr < prev_avg_ctr * 0.8:
                    drop_pct = (prev_avg_ctr - curr_avg_ctr) / prev_avg_ctr * 100
                    efficiency_details.append(f"CTR -{drop_pct:.0f}%")
                if prev_avg_cpc > 0 and curr_avg_cpc > prev_avg_cpc * 1.25:
                    rise_pct = (curr_avg_cpc - prev_avg_cpc) / prev_avg_cpc * 100
                    efficiency_details.append(f"CPC +{rise_pct:.0f}%")
                
                reason_str = " & ".join(efficiency_details)
                if not reason_str: reason_str = "Efficiency Alert"

                anomalies.append({
                    "id": str(item_id),
                    "item_id": str(item_id),
                    "title": str(title)[:50],
                    "date": last_date.strftime('%Y-%m-%d'),
                    "curr_cost": float(curr_cost),
                    "prev_cost": float(prev_cost),
                    "curr_clicks": float(curr_clicks_sum),
                    "prev_clicks": float(prev_clicks_sum),
                    "curr_ctr": float(curr_avg_ctr) if not pd.isna(curr_avg_ctr) else 0.0,
                    "prev_ctr": float(prev_avg_ctr) if not pd.isna(prev_avg_ctr) else 0.0,
                    # Using CTR/CPC instead of ROAS/CPA for frontend display
                    "current_conv": float(current_clicks),  # clicks as proxy for conversions
                    "prev_conv": float(prev_clicks),
                    "curr_roas": float(curr_avg_ctr) if not pd.isna(curr_avg_ctr) else 0.0,  # CTR as proxy
                    "prev_roas": float(prev_avg_ctr) if not pd.isna(prev_avg_ctr) else 0.0,
                    "curr_cpa": float(curr_avg_cpc) if not pd.isna(curr_avg_cpc) else 0.0,  # CPC as proxy
                    "prev_cpa": float(prev_avg_cpc) if not pd.isna(prev_avg_cpc) else 0.0,
                    "reason": f"{reason_str} & No Growth"
                })
        
        # Sort by cost (highest cost issues first)
        anomalies.sort(key=lambda x: x['curr_cost'], reverse=True)
        print(f"Product Anomalies: Detected {len(anomalies)} total")
        return anomalies  # Return all anomalies (pagination handled by API)

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
            
            # Robust Fallback for ID (some models don't return tool_call_id)
            if not tool_id:
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
你的职责是实时监控广告表现，并协调"专项专家"进行深入诊断。

**当前活跃的专项专家 (仅限以下):**
{expertise_section}

**时间维度决策:**
- 你必须根据用户的提问（如"分析本周"、"分析1月1日到18日"）或者通过上下文感知来决定 `start_date` 和 `end_date`。
- 如果用户没有指定，默认使用数据截止日期（如 2026-01-18）的前7天。

**工作流程与汇报原则:**
1. **显性化思考**: 在调用任何工具前，你必须先输出一段分析思路。
2. **⚠️ 强制全量汇报 (CRITICAL)**: 
   - 当 `scan_campaigns_for_anomalies` 返回 N 个异常时，你的最终报告【必须】包含【全部 N 个】异常系列。
   - 🚨 **严禁省略**：如果扫描返回4个异常，你必须报告4个，不能只报告1个。
3. **多维专家调度**: 对于每一个被检测出的异常系列，根据其根本原因调遣专家。
4. **汇总报告**: 对于每个异常系列都需要单独成节汇报。

**原则:**
- 只有用户勾选了某个表的Agent，你才能调遣该专家。
- 输出必须专业、准确、全中文。
""")
            messages = [system_prompt] + messages
            
        # SANITIZE HISTORY
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
            
            # Define preferred column order (important columns first)
            COLUMN_ORDER = {
                'campaign': ['date', 'campaign', 'campaign_status', 'roas', 'roas_before_7d_average', 'roas_compare', 'cpa', 'cpa_before_7d_average', 'cpa_compare', 'conversions', 'conv_value', 'cost', 'clicks', 'impressions', 'ctr', 'conversions_rate', 'budget', 'campaign_type', 'search_impr_share'],
                'product': ['date', 'title', 'item_id', 'ctr', 'ctr_before_7d_average', 'ctr_compare', 'avg_cpc', 'cpc_before_7d_average', 'cpc_compare', 'cost', 'clicks', 'impr', 'price', 'status', 'issues', 'merchant_id'],
                'search_term': ['date', 'search_term', 'conversions', 'cost', 'clicks', 'impressions', 'ctr', 'campaign', 'ad_group'],
            }
            
            # Reorder columns if table has configured order
            if table_name in COLUMN_ORDER:
                preferred = COLUMN_ORDER[table_name]
                # Create ordered list: preferred columns first (in order), then remaining columns
                ordered = []
                for col in preferred:
                    if col in display_columns:
                        ordered.append(col)
                for col in display_columns:
                    if col not in ordered:
                        ordered.append(col)
                display_columns = ordered
            
            data = [dict(row) for row in rows]
            
            # Post-process product data to add comparison columns (like campaign has roas_compare, cpa_compare)
            if table_name == 'product' and data:
                import pandas as pd
                df = pd.DataFrame(data)
                
                # Clean numeric columns
                for col in ['ctr', 'avg_cpc']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace('%', '').str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
                
                if 'item_id' in df.columns and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values(['item_id', 'date'])
                    
                    # Calculate 7-day rolling average for each product
                    if 'ctr' in df.columns:
                        df['ctr_before_7d_average'] = df.groupby('item_id')['ctr'].transform(
                            lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
                        ).fillna(0)
                        # CTR compare: current - average (higher is better, so positive=good)
                        df['ctr_compare'] = df['ctr'] - df['ctr_before_7d_average']
                    
                    if 'avg_cpc' in df.columns:
                        df['cpc_before_7d_average'] = df.groupby('item_id')['avg_cpc'].transform(
                            lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
                        ).fillna(0)
                        # CPC compare: current - average (lower is better, so negative=good)
                        df['cpc_compare'] = df['avg_cpc'] - df['cpc_before_7d_average']
                    
                    # Round for display
                    for col in ['ctr_before_7d_average', 'ctr_compare', 'cpc_before_7d_average', 'cpc_compare']:
                        if col in df.columns:
                            df[col] = df[col].round(2)
                    
                    # Convert date back to string format (YYYY-MM-DD) to avoid T00:00:00
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    
                    # Sort by date descending (most recent first) as default
                    df = df.sort_values('date', ascending=False)
                    
                    data = df.to_dict('records')
                    
                    # Update display columns to include new comparison columns
                    new_cols = ['ctr_before_7d_average', 'ctr_compare', 'cpc_before_7d_average', 'cpc_compare']
                    # Insert after ctr and avg_cpc
                    updated_columns = []
                    for col in display_columns:
                        updated_columns.append(col)
                        if col == 'ctr':
                            updated_columns.extend(['ctr_before_7d_average', 'ctr_compare'])
                        elif col == 'avg_cpc':
                            updated_columns.extend(['cpc_before_7d_average', 'cpc_compare'])
                    display_columns = updated_columns
            
            return {"columns": display_columns, "data": data}
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()

    def get_campaign_anomalies(self, target_date: str = None):
        """Wrapper for standalone logic"""
        return get_campaign_anomalies_logic(target_date)

    def get_campaign_analyzable_date_range(self):
        """
        Get the analyzable date range for campaign anomalies.
        A date is analyzable if it has at least 10 days of prior data (3 check + 7 history).
        """
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(date), MAX(date) FROM campaign")
            result = cursor.fetchone()
            
            if result and result[0] and result[1]:
                from datetime import datetime, timedelta
                min_data_date = datetime.strptime(result[0], '%Y-%m-%d')
                max_data_date = datetime.strptime(result[1], '%Y-%m-%d')
                
                # Analyzable min date = earliest data date + 10 days
                analyzable_min = min_data_date + timedelta(days=10)
                
                return {
                    "min_date": analyzable_min.strftime('%Y-%m-%d'),
                    "max_date": max_data_date.strftime('%Y-%m-%d'),
                    "data_start": result[0],
                    "data_end": result[1]
                }
            return {"min_date": None, "max_date": None}
        except Exception as e:
            print(f"Error getting campaign date range: {e}")
            return {"min_date": None, "max_date": None, "error": str(e)}
        finally:
            conn.close()


    def get_product_anomalies(self, target_date: str = None):
        """Wrapper for product anomaly detection"""
        return get_product_anomalies_logic(target_date)

    def get_product_analyzable_date_range(self):
        """
        Get the analyzable date range for product anomalies.
        A date is analyzable if it has at least 7 days of prior data.
        Returns: {min_date, max_date} where min_date = data_start + 7 days
        """
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(date), MAX(date) FROM product")
            result = cursor.fetchone()
            
            if result and result[0] and result[1]:
                from datetime import datetime, timedelta
                min_data_date = datetime.strptime(result[0], '%Y-%m-%d')
                max_data_date = datetime.strptime(result[1], '%Y-%m-%d')
                
                # Analyzable min date = earliest data date + 10 days
                # (need 3 check days + 7 prior days for average calculation)
                analyzable_min = min_data_date + timedelta(days=10)
                
                return {
                    "min_date": analyzable_min.strftime('%Y-%m-%d'),
                    "max_date": max_data_date.strftime('%Y-%m-%d'),
                    "data_start": result[0],
                    "data_end": result[1]
                }
            return {"min_date": None, "max_date": None}
        except Exception as e:
            print(f"Error getting date range: {e}")
            return {"min_date": None, "max_date": None, "error": str(e)}
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

    def get_campaign_anomaly_details(self, campaign_name: str, start_date: str = None, end_date: str = None):
        """
        严格按照图片规则判定异常:
        【触发条件】
        - ROAS 连续 3 天低于 20%
        - 或 CPA 连续 3 天高于 7 天均值 ≥25%
        - 同期转化量未同比增长
        【专业经验】
        - Display占比>35%且ROAS<全账户均值50% → Pmax吃低质流量
        - 广泛匹配占比增加 + Search Term CVR下降 → 流量质量下降
        """
        result = {}
        conn = get_db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 使用end_date作为目标日期，如果没有则用今天
        target_date = end_date or start_date or '2026-01-18'
        
        date_filter = ""
        date_params = []
        if start_date:
            date_filter += " AND date >= ?"
            date_params.append(start_date)
        if end_date:
            date_filter += " AND date <= ?"
            date_params.append(end_date)
        
        try:
            # =========================================================================
            # 第一步: 账户级基准数据
            # =========================================================================
            
            # 账户7天平均ROAS
            cursor.execute("""
                SELECT AVG(CAST(conv_value AS REAL) / NULLIF(CAST(cost AS REAL), 0)) as avg_roas
                FROM campaign WHERE CAST(cost AS REAL) > 0
            """)
            row = cursor.fetchone()
            account_avg_roas = row['avg_roas'] if row and row['avg_roas'] else 2.0
            
            # =========================================================================
            # 第二步: 【图片规则1】ROAS 连续 3 天低于 20% 检测
            # =========================================================================
            cursor.execute("""
                SELECT date, 
                       SUM(CAST(conv_value AS REAL)) / NULLIF(SUM(CAST(cost AS REAL)), 0) as daily_roas
                FROM campaign 
                WHERE campaign = ? AND date >= date(?, '-3 days') AND date <= ?
                GROUP BY date ORDER BY date DESC
            """, (campaign_name, target_date, target_date))
            daily_roas_rows = cursor.fetchall()
            
            roas_3day_anomaly = False
            roas_3day_values = []
            if len(daily_roas_rows) >= 3:
                for r in daily_roas_rows[:3]:
                    daily_r = r['daily_roas'] or 0
                    roas_3day_values.append(round(daily_r, 2))
                    # 低于账户均值20%
                    if daily_r < account_avg_roas * 0.2:
                        pass  # 单日检测
                # 连续3天都低于20%才算异常
                roas_3day_anomaly = all(r < account_avg_roas * 0.2 for r in roas_3day_values)
            
            # =========================================================================
            # 第三步: 【图片规则2】CPA 连续 3 天高于 7 天均值 ≥25%
            # =========================================================================
            # 计算7天平均CPA
            cursor.execute("""
                SELECT SUM(CAST(cost AS REAL)) / NULLIF(SUM(CAST(conversions AS REAL)), 0) as avg_cpa_7d
                FROM campaign 
                WHERE campaign = ? AND date >= date(?, '-7 days') AND date <= ?
            """, (campaign_name, target_date, target_date))
            row = cursor.fetchone()
            avg_cpa_7d = row['avg_cpa_7d'] if row and row['avg_cpa_7d'] else 50.0
            
            # 查询最近3天每日CPA
            cursor.execute("""
                SELECT date,
                       SUM(CAST(cost AS REAL)) / NULLIF(SUM(CAST(conversions AS REAL)), 0) as daily_cpa
                FROM campaign 
                WHERE campaign = ? AND date >= date(?, '-3 days') AND date <= ?
                GROUP BY date ORDER BY date DESC
            """, (campaign_name, target_date, target_date))
            daily_cpa_rows = cursor.fetchall()
            
            cpa_3day_anomaly = False
            cpa_3day_values = []
            cpa_threshold = avg_cpa_7d * 1.25  # 高于25%
            if len(daily_cpa_rows) >= 3:
                for r in daily_cpa_rows[:3]:
                    daily_c = r['daily_cpa'] or 0
                    cpa_3day_values.append(round(daily_c, 2))
                # 连续3天都高于7天均值25%才算异常
                cpa_3day_anomaly = all(c > cpa_threshold for c in cpa_3day_values if c > 0)
            
            # =========================================================================
            # 第四步: 【图片规则3】广泛匹配占比增加 + CVR下降 检测 (7天 vs 前7天)
            # =========================================================================
            # 当前7天
            cursor.execute("""
                SELECT SUM(CAST(conversions AS REAL)) as conv,
                       SUM(CAST(interactions AS REAL)) as clicks,
                       SUM(CASE WHEN LOWER(match_type) LIKE '%broad%' THEN CAST(cost AS REAL) ELSE 0 END) as broad_cost,
                       SUM(CAST(cost AS REAL)) as total_cost
                FROM search_term
                WHERE campaign = ? AND date >= date(?, '-7 days') AND date <= ?
            """, (campaign_name, target_date, target_date))
            current = cursor.fetchone()
            
            # 前7天 (14天前到7天前)
            cursor.execute("""
                SELECT SUM(CAST(conversions AS REAL)) as conv,
                       SUM(CAST(interactions AS REAL)) as clicks,
                       SUM(CASE WHEN LOWER(match_type) LIKE '%broad%' THEN CAST(cost AS REAL) ELSE 0 END) as broad_cost,
                       SUM(CAST(cost AS REAL)) as total_cost
                FROM search_term
                WHERE campaign = ? AND date >= date(?, '-14 days') AND date < date(?, '-7 days')
            """, (campaign_name, target_date, target_date))
            previous = cursor.fetchone()
            
            broad_cvr_anomaly = False
            current_broad_share = 0
            prev_broad_share = 0
            current_cvr = 0
            prev_cvr = 0
            
            if current and previous:
                current_clicks = current['clicks'] or 0
                current_conv = current['conv'] or 0
                current_total_cost = current['total_cost'] or 0
                current_broad_cost = current['broad_cost'] or 0
                
                prev_clicks = previous['clicks'] or 0
                prev_conv = previous['conv'] or 0
                prev_total_cost = previous['total_cost'] or 0
                prev_broad_cost = previous['broad_cost'] or 0
                
                current_cvr = current_conv / current_clicks if current_clicks > 0 else 0
                prev_cvr = prev_conv / prev_clicks if prev_clicks > 0 else 0
                current_broad_share = current_broad_cost / current_total_cost if current_total_cost > 0 else 0
                prev_broad_share = prev_broad_cost / prev_total_cost if prev_total_cost > 0 else 0
                
                # 广泛匹配占比增加(>5%) + CVR下降(>20%)
                broad_increase = current_broad_share - prev_broad_share
                cvr_decline = (prev_cvr - current_cvr) / prev_cvr if prev_cvr > 0 else 0
                
                broad_cvr_anomaly = broad_increase > 0.05 and cvr_decline > 0.20
            
            # =========================================================================
            # 存储趋势检测结果到 _baseline
            # =========================================================================
            result['_baseline'] = {
                'account_avg_roas': round(account_avg_roas, 2),
                'avg_cpa_7d': round(avg_cpa_7d, 2),
                # 图片规则1: ROAS连续3天
                'roas_3day_values': roas_3day_values,
                'roas_3day_anomaly': roas_3day_anomaly,
                'roas_3day_threshold': round(account_avg_roas * 0.2, 2),
                # 图片规则2: CPA连续3天
                'cpa_3day_values': cpa_3day_values,
                'cpa_3day_anomaly': cpa_3day_anomaly,
                'cpa_threshold': round(cpa_threshold, 2),
                # 图片规则3: 广泛匹配+CVR
                'current_broad_share': round(current_broad_share * 100, 1),
                'prev_broad_share': round(prev_broad_share * 100, 1),
                'current_cvr': round(current_cvr * 100, 2),
                'prev_cvr': round(prev_cvr * 100, 2),
                'broad_cvr_anomaly': broad_cvr_anomaly
            }
            
            # =========================================================================
            # 1. Search Term - 规则: 垃圾词 + 广泛匹配CVR下降 + 高消耗零转化
            # =========================================================================
            junk_patterns = ['free', 'repair', 'login', 'support', 'manual', 'review', 'whatsapp', 'tutorial', 'how to', 'what is', 'download', 'crack', 'hack']
            junk_pattern_sql = " OR ".join([f"LOWER(search_term) LIKE '%{p}%'" for p in junk_patterns])
            
            # 计算Campaign平均CVR作为基准 (使用interactions作为clicks)
            cursor.execute(f"""
                SELECT SUM(CAST(conversions AS REAL)) as total_conv, 
                       SUM(CAST(interactions AS REAL)) as total_clicks
                FROM search_term 
                WHERE campaign = ? {date_filter}
            """, (campaign_name, *date_params))
            cvr_row = cursor.fetchone()
            total_conv = cvr_row['total_conv'] or 0
            total_clicks = cvr_row['total_clicks'] or 0
            campaign_avg_cvr = total_conv / total_clicks if total_clicks > 0 else 0
            cvr_threshold = campaign_avg_cvr * 0.5  # 低于均值50%视为异常
            
            # 查询异常搜索词: 垃圾词 或 高消耗零转化 或 CVR低于均值50%
            query = f"""
                SELECT *, 
                    (CAST(conversions AS REAL) / NULLIF(CAST(interactions AS REAL), 0)) as cvr
                FROM search_term 
                WHERE campaign = ? {date_filter}
                AND (
                    ({junk_pattern_sql})
                    OR (CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0)
                    OR (CAST(interactions AS REAL) > 1 AND (CAST(conversions AS REAL) / NULLIF(CAST(interactions AS REAL), 0)) < ?)
                )
                ORDER BY CAST(cost AS REAL) DESC
                LIMIT 50
            """
            cursor.execute(query, (campaign_name, *date_params, cvr_threshold))
            rows = cursor.fetchall()
            if rows:
                columns = [d[0] for d in cursor.description]
                result['search_term'] = {
                    "columns": columns, 
                    "data": [dict(r) for r in rows], 
                    "rule": f"垃圾词/CVR<{cvr_threshold*100:.1f}%/高消耗零转化",
                    "anomaly_count": len(rows),
                    "campaign_avg_cvr": round(campaign_avg_cvr * 100, 2)
                }
            else:
                result['search_term'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            
            # 2. Channel - 规则: Display占比>35%且ROAS<均值50% (Pmax吃低质流量)
            # =========================================================================
            try:
                # 先计算该Campaign各渠道的花费和ROAS
                cursor.execute(f"""
                    SELECT channels, 
                           SUM(CAST(cost AS REAL)) as cost, 
                           SUM(CAST(results_value AS REAL)) as value,
                           SUM(CAST(conversions AS REAL)) as conversions
                    FROM channel 
                    WHERE campaigns LIKE ? {date_filter}
                    GROUP BY channels
                """, (f"%{campaign_name}%", *date_params))
                channel_rows = cursor.fetchall()
                
                total_channel_cost = sum(r['cost'] or 0 for r in channel_rows)
                total_channel_value = sum(r['value'] or 0 for r in channel_rows)
                camp_roas = total_channel_value / total_channel_cost if total_channel_cost > 0 else 0
                
                # 计算Display占比
                display_cost = sum(r['cost'] or 0 for r in channel_rows if r['channels'] and 'display' in r['channels'].lower())
                display_ratio = display_cost / total_channel_cost if total_channel_cost > 0 else 0
                
                # 筛选异常渠道: Display/Video占比>35%且ROAS<Campaign均值50%
                anomaly_channels = []
                for r in channel_rows:
                    ch_name = r['channels'] or ''
                    ch_cost = r['cost'] or 0
                    ch_value = r['value'] or 0
                    ch_roas = ch_value / ch_cost if ch_cost > 0 else 0
                    ch_share = ch_cost / total_channel_cost if total_channel_cost > 0 else 0
                    
                    # 规则: 占比>35% 且 ROAS<均值50%
                    is_anomaly = ch_share > 0.35 and ch_roas < (camp_roas * 0.5)
                    # 或: 高消耗零转化
                    is_high_cost_zero = ch_cost > 50 and (r['conversions'] or 0) == 0
                    
                    if is_anomaly or is_high_cost_zero:
                        anomaly_channels.append({
                            'channels': ch_name,
                            'cost': ch_cost,
                            'value': ch_value,
                            'conversions': r['conversions'] or 0,
                            'roas': round(ch_roas, 2),
                            'share': round(ch_share * 100, 1),
                            'anomaly_reason': '占比>35%且ROAS低' if is_anomaly else '高消耗零转化'
                        })
                
                # 构建规则描述
                rule_desc = f"Display占比{display_ratio*100:.0f}%"
                if display_ratio > 0.35:
                    rule_desc += ">35% (Pmax吃低质流量风险)"
                
                if anomaly_channels:
                    result['channel'] = {
                        "columns": ['channels', 'cost', 'value', 'conversions', 'roas', 'share', 'anomaly_reason'],
                        "data": anomaly_channels,
                        "rule": rule_desc,
                        "anomaly_count": len(anomaly_channels),
                        "display_ratio": round(display_ratio * 100, 1),
                        "camp_roas": round(camp_roas, 2)
                    }
                else:
                    result['channel'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0, "display_ratio": round(display_ratio * 100, 1)}
            except Exception as e:
                result['channel'] = {"columns": [], "data": [], "rule": f"查询错误: {str(e)}", "anomaly_count": 0}
            
            # 3. Audience - 只返回高消耗零转化受众 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM audience 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                    LIMIT 30
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['audience'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "Cost>$1 且 Conv=0",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['audience'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['audience'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
            # 4. Location - 只返回预算黑洞 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM location_by_cities_all_campaign 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                    LIMIT 30
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['location_by_cities_all_campaign'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "预算黑洞",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['location_by_cities_all_campaign'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['location_by_cities_all_campaign'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
            # 5. Age - 只返回低效年龄段 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM age 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['age'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "Cost>$1 且 Conv=0",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['age'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['age'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
            # 6. Gender - 只返回低效性别 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM gender 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['gender'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "Cost>$1 且 Conv=0",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['gender'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['gender'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
            # 7. Ad Schedule - 只返回低效时段 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM ad_schedule 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                    LIMIT 30
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['ad_schedule'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "低效时段",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['ad_schedule'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['ad_schedule'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
            # 8. Asset - 只返回低效素材 (仅异常行)
            # =========================================================================
            try:
                query = f"""
                    SELECT * FROM asset 
                    WHERE campaign = ? {date_filter}
                    AND CAST(cost AS REAL) > 1 AND CAST(conversions AS REAL) = 0
                    ORDER BY CAST(cost AS REAL) DESC
                    LIMIT 30
                """
                cursor.execute(query, (campaign_name, *date_params))
                rows = cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    result['asset'] = {
                        "columns": columns, 
                        "data": [dict(r) for r in rows], 
                        "rule": "低效素材",
                        "anomaly_count": len(rows)
                    }
                else:
                    result['asset'] = {"columns": [], "data": [], "rule": "无异常", "anomaly_count": 0}
            except:
                result['asset'] = {"columns": [], "data": [], "rule": "表不存在", "anomaly_count": 0}
            
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
                "data_source": knowledge.get("data_source", ""),
                "extraction_method": knowledge.get("extraction_method", "").strip(),
                "hard_rules": knowledge.get("hard_rules", "").strip(),
                "default_prompt": f"""【{knowledge.get("title", "")}】
专注领域: {knowledge.get("focus", "")}
数据来源: {knowledge.get("data_source", "")}

数据提取方法:
{knowledge.get("extraction_method", "").strip()}

硬规则判定:
{knowledge.get("hard_rules", "").strip()}"""
            }
        else:
            return {"error": f"Unknown agent: {table_name}", "default_prompt": ""}

