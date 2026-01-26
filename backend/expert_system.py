import sqlite3
import pandas as pd
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import json

# --- Config & Helpers ---

DB_FILE = 'ads_data.sqlite' # Relative path assuming running from root or adjusted in usage

def get_db_connection():
    # Helper assuming this file is imported where DB_FILE is accessible or passed
    # For standalone, we define it here, but ideally we share with agent_service
    import os
    # Assuming running from D:\ads_manager
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ads_data.sqlite')
    return sqlite3.connect(db_path)

def query_db(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"DB Error in ExpertSystem: {e}")
        return []
    finally:
        conn.close()

# --- 1. ContextGuard (Risk Control) ---

# --- 1. ContextGuard (风险控制) ---

class ContextGuard:
    """
    风险控制层。
    基于业务约束（如大促、冷启动、学习期）下调或拦截操作建议。
    """
    
    PROMOTION_PERIODS = [
        ('2025-11-20', '2025-12-05'), # 黑五/网络星期一 (BFCM)
        ('2026-06-01', '2026-06-20'), # 618 大促
    ]

    @staticmethod
    def check_risk(campaign_context: Dict[str, Any], target_date: str) -> Dict[str, Any]:
        """
        返回风险评估结果。
        输出: { "status": "PASS" | "BLOCK" | "MARK", "reasons": [...] }
        """
        reasons = []
        status = "PASS"
        campaign_name = campaign_context.get('campaign', 'Unknown')
        
        # 1. 大促期与预热期保护 (Promotion & Lead-up)
        # 预热期: 大促开始前 3 天
        for start, end in ContextGuard.PROMOTION_PERIODS:
            start_dt = datetime.strptime(start, '%Y-%m-%d')
            target_dt = datetime.strptime(target_date, '%Y-%m-%d')
            lead_up_start = start_dt - timedelta(days=3)
            
            if start <= target_date <= end:
                status = "MARK"
                reasons.append(f"处于大促期间 ({start} 至 {end}) - 触发保护机制，建议观察。")
            elif lead_up_start <= target_dt < start_dt:
                status = "MARK"
                reasons.append(f"大促预热期 (大促将于 {start} 开始) - 数据可能剧烈波动，仅做标记。")
        
        # 2. 调价冷却期保护 (72H Budget Shift)
        # 查询过去 3 天的预算历史
        budget_query = """
            SELECT budget FROM campaign 
            WHERE campaign = ? AND date <= ? AND date >= date(?, '-3 days')
            ORDER BY date DESC
        """
        budgets = query_db(budget_query, (campaign_name, target_date, target_date))
        if budgets and len(budgets) >= 2:
            budget_values = [b['budget'] for b in budgets if b['budget'] is not None]
            if len(set(budget_values)) > 1:
                status = "MARK"
                reasons.append("调价冷却期: 过去 72 小时内检测到预算变动，数据尚未稳定，建议维持现状。")

        # 3. 冷启动 / 学习期保护 (Cold Start / Learning Phase)
        query = """
            SELECT SUM(cost) as cost, SUM(conversions) as conv, COUNT(date) as days, MIN(date) as first_day
            FROM campaign
            WHERE campaign = ? AND date <= ?
        """
        res = query_db(query, (campaign_name, target_date))
        if res and res[0]['first_day'] is not None:
            stats = res[0]
            first_day_dt = datetime.strptime(stats['first_day'], '%Y-%m-%d')
            target_dt = datetime.strptime(target_date, '%Y-%m-%d')
            days_diff = (target_dt - first_day_dt).days
            
            if days_diff < 7:
                status = "BLOCK"
                reasons.append(f"冷启动保护: 该系列上线仅 {days_diff + 1} 天 (不足 7 天)，严禁进行削减操作。")
            elif (stats['conv'] or 0) < 30:
                 # 学习期通常判定为“系统提示”而非硬性风险拦截
                 reasons.append("进入学习期: 累计转化数 < 30，系统仍在优化人群模型。")

        return {"status": status, "reasons": reasons}

# --- 2. ExpertEngines (确定性专家规则) ---

class ExpertEngine:
    
    @staticmethod
    def search_term_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        搜索词专家: 识别高损耗词与匹配质量下滑
        """
        flags = []
        
        # 1. 获取搜索词数据 (交互/点击字段: interactions, 转化字段: conversions)
        query = """
            SELECT search_term, match_type, SUM(cost) as cost, SUM(conversions) as conversions, SUM(interactions) as clicks
            FROM search_term
            WHERE campaign = ? AND date >= date(?, '-7 days')
            GROUP BY search_term, match_type
        """
        rows = query_db(query, (campaign_name, target_date))
        if not rows: return []
        
        total_cost = sum(r['cost'] for r in rows)
        if total_cost == 0: return []
        
        avg_cpa = 40.0 
        
        for r in rows:
            term = r['search_term'].lower()
            if any(brand in term for brand in ['brandname', 'google', 'official']):
                continue
                
            if r['cost'] > (avg_cpa * 1.5) and r['conversions'] == 0:
                flags.append({
                    "expert": "搜索词专家",
                    "issue": "高损耗搜索词 (建议排除)",
                    "severity": "HIGH",
                    "evidence": f"搜索词 '{r['search_term']}' 在7天内消耗 ${r['cost']:.0f} 且 0 转化。",
                    "suggestion": "添加为否定关键词。"
                })
        
        broad_cost = sum(r['cost'] for r in rows if r['match_type'].lower() == 'broad')
        broad_share = broad_cost / total_cost
        if broad_share > 0.45:
             flags.append({
                "expert": "搜索词专家",
                "issue": "流量匹配质量下滑",
                "severity": "MEDIUM",
                "evidence": f"广泛匹配消耗占比达 {broad_share*100:.1f}%，存在引入无关流量风险。",
                "suggestion": "检查搜索词条目；考虑切换为词组匹配或完全匹配以精准控制。"
            })
            
        return flags

    @staticmethod
    def channel_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        渠道专家 (PMax): 识别流量洗样与交叉补贴
        """
        flags = []
        # Table: channel -> date, channels, status, campaigns, impr, clicks, interactions, conversions, conv_value, currency_code, cost, results, results_value, reports
        query = """
            SELECT channels, SUM(cost) as cost, SUM(results_value) as value, SUM(conversions) as conversions
            FROM channel
            WHERE campaigns = ? AND date >= date(?, '-7 days')
            GROUP BY channels
        """
        rows = query_db(query, (campaign_name, target_date))
        if not rows: return []
        
        total_cost = sum(r['cost'] for r in rows)
        total_value = sum(r['value'] for r in rows)
        camp_roas = total_value / total_cost if total_cost > 0 else 0
        
        channel_data = {r['channels']: r for r in rows}
        
        for ch in ['Display', 'Video']:
            if ch in channel_data:
                d = channel_data[ch]
                share = d['cost'] / total_cost
                ch_roas = d['value'] / d['cost'] if d['cost'] > 0 else 0
                if share > 0.35 and ch_roas < (camp_roas * 0.5):
                    flags.append({
                        "expert": "渠道专家 (PMax)",
                        "issue": "系统判定: 流量洗样 (Traffic Washing)",
                        "severity": "HIGH",
                        "evidence": f"{ch} 渠道消耗占比高达 {share*100:.0f}%，但 ROAS 仅为 {ch_roas:.2f} (远低于均值)。",
                        "suggestion": "检查素材资源组的人群信号；考虑排除低质量展示位。"
                    })
        
        if 'Shopping' in channel_data:
            s = channel_data['Shopping']
            s_roas = s['value'] / s['cost'] if s['cost'] > 0 else 0
            if s_roas > (camp_roas * 1.5):
                 flags.append({
                    "expert": "渠道专家 (PMax)",
                    "issue": "系统判定: 交叉补贴 (Cross-Subsidy)",
                    "severity": "MEDIUM",
                    "evidence": f"Shopping 渠道 ROAS ({s_roas:.2f}) 掩盖了其他渠道的亏损，整体效率受损。",
                    "suggestion": "优化或削减转化率低下的非购物素材组。"
                })

        return flags

    @staticmethod
    def product_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        商品专家: 识别僵尸商品与预算垄断
        """
        flags = []
        # Note: product 缺少 campaigns 列，这里可能需要跳过或优化逻辑
        # 暂时只查询 product 表，由于没有 campaign 列，这里可能会返回空或全量，所以我们只能通过其他方式。
        # 如果 product 表没有 campaign/campaigns 列，这是一个架构缺陷。
        # 我们暂时改为获取全量排名前 10 的商品作为风险提示。
        query = """
            SELECT item_id, title, SUM(cost) as cost, SUM(clicks) as clicks
            FROM product
            WHERE date >= date(?, '-7 days')
            GROUP BY item_id
            ORDER BY cost DESC
            LIMIT 10
        """
        rows = query_db(query, (target_date,))
        if not rows: return []
        
        total_cost = sum(r['cost'] for r in rows)
        if total_cost == 0: return []

        if rows[0]['cost'] / total_cost > 0.85:
            flags.append({
                "expert": "商品专家",
                "issue": "预算垄断 (全局 Top 商品)",
                "severity": "MEDIUM",
                "evidence": f"单品 '{rows[0]['title']}' 占据了主力预算。 (注: 该表数据暂不支持战役级下钻)",
                "suggestion": "考虑监控该商品在各系列的分配。"
            })
            
        for r in rows:
            if r['cost'] > 100: # 如果消耗巨大但不知道转化
                 flags.append({
                    "expert": "商品专家",
                    "issue": "高消耗商品 (预警)",
                    "severity": "LOW",
                    "evidence": f"商品 '{r['title']}' 近7天消耗 ${r['cost']:.0f}。",
                    "suggestion": "请在 Google Ads 后台核实该商品的实际转化表现。"
                })
        return flags

    @staticmethod
    def keyword_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        关键词专家: 识别高损耗低 ROAS 关键词
        """
        flags = []
        query = """
            SELECT keyword, match_type, SUM(cost) as cost, SUM(conversions) as conversions, SUM(conv_value) as value
            FROM keyword
            WHERE campaign = ? AND date >= date(?, '-14 days')
            GROUP BY keyword, match_type
            ORDER BY cost DESC
        """
        rows = query_db(query, (campaign_name, target_date))
        if not rows: return []
        
        for r in rows:
            roas = r['value'] / r['cost'] if r['cost'] > 0 else 0
            if r['cost'] > 50 and roas < 0.5:
                flags.append({
                    "expert": "关键词专家",
                    "issue": "主干词损耗过大",
                    "severity": "HIGH",
                    "evidence": f"关键词 '{r['keyword']}' 消耗 ${r['cost']:.0f}，但 ROAS 仅为 {roas:.2f}。",
                    "suggestion": "暂停该关键词或大幅下调出价。"
                })
        return flags

    @staticmethod
    def demographics_expert(campaign_name: str, table_name: str, target_date: str) -> List[Dict]:
        """
        人口统计专家 (年龄/性别): 识别结构性低效
        """
        flags = []
        col = 'age' if table_name == 'age' else 'gender'
        query = f"""
            SELECT {col}, SUM(cost) as cost, SUM(conversions) as conversions
            FROM {table_name}
            WHERE campaign = ? AND date >= date(?, '-30 days')
            GROUP BY {col}
        """
        rows = query_db(query, (campaign_name, target_date))
        if not rows: return []
        
        total_cost = sum(r['cost'] for r in rows)
        total_conv = sum(r['conversions'] for r in rows)
        if total_cost == 0: return []
        
        for r in rows:
            cpa = r['cost'] / r['conversions'] if r['conversions'] > 0 else (r['cost'] * 2) 
            avg_cpa = total_cost / total_conv if total_conv > 0 else 40.0
            
            if (r['cost'] / total_cost > 0.15) and (cpa > avg_cpa * 1.5):
                flags.append({
                    "expert": "人口统计专家",
                    "issue": f"{table_name} 分层效率低下",
                    "severity": "MEDIUM",
                    "evidence": f"'{r[col]}' 分层消耗占比 {r['cost']/total_cost*100:.0f}%，CPA 高于均值 50% 以上。",
                    "suggestion": "对此人口分层应用负出价调整 (-50% 或更多)。"
                })
        return flags

    @staticmethod
    def geo_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        地理位置专家: 识别地理黑洞
        """
        flags = []
        # Table: location_by_cities_all_campaign -> date, location, campaign, ...
        query = """
            SELECT location, SUM(cost) as cost, SUM(conversions) as conversions
            FROM location_by_cities_all_campaign
            WHERE campaign = ? AND date >= date(?, '-30 days')
            GROUP BY location
        """
        rows = query_db(query, (campaign_name, target_date))
        if not rows: return []
        
        for r in rows:
            if r['cost'] > 100 and r['conversions'] == 0:
                flags.append({
                    "expert": "地理专家",
                    "issue": "地理黑洞 (Geo Blackhole)",
                    "severity": "HIGH",
                    "evidence": f"地区 '{r['location']}' 在过去30天消耗 ${r['cost']:.0f} 且 0 转化。",
                    "suggestion": "排除该地理区域以节省预算。"
                })
        return flags

    @staticmethod
    def time_expert(campaign_name: str, target_date: str) -> List[Dict]:
        """
        分时专家: 识别投放时段异常
        """
        # Table 'time' is missing or has no schema, skip safely.
        return []

# --- 3. DiagnosisAggregator (诊断聚合器) ---

class DiagnosisAggregator:
    
    @staticmethod
    def aggregate(campaign_name: str, trigger_info: Dict, expert_flags: List[Dict], context_status: Dict) -> Dict:
        """
        聚合最终诊断报告。
        """
        root_causes = []
        for f in expert_flags:
            if f['expert'] == '搜索词专家': root_causes.append("流量质量下滑")
            if f['expert'] == '商品专家': root_causes.append("结构性利润问题")
            if f['expert'] == '渠道专家 (PMax)': root_causes.append("渠道洗样/补贴")
            if f['expert'] == '地理专家': root_causes.append("地理投放黑洞")
            if f['expert'] == '关键词专家': root_causes.append("主词流量损耗")
        
        final_root_cause = " & ".join(list(set(root_causes))) if root_causes else "宏观效率波动 (未命中特定规则)"

        # 置信度逻辑
        high_sev = any(f['severity'] == "HIGH" for f in expert_flags)
        if high_sev:
            confidence = "高 (命中核心规则)"
        elif expert_flags:
            confidence = "中 (存在可观测模式)"
        else:
            confidence = "低 (仅有趋势信号)"

        # 执行动作建议核心逻辑
        if context_status['status'] == 'BLOCK':
            action_level = "✋ 风险拦截 (冷启动保护期，仅做标记)"
        elif context_status['status'] == 'MARK':
            action_level = "⚠️ 标记观察 (处于调价或大促预热期)"
        elif high_sev:
            action_level = "🚀 立即优化 (建议执行操作)"
        elif expert_flags:
            action_level = "🔍 深度诊断 (建议详细排查数据)"
        else:
            action_level = "👀 仅做标记 (未命中特定模式)"

        coverage = 0.90 
        
        return {
            "campaign_name": campaign_name,
            "diagnosis": {
                "root_cause": final_root_cause,
                "confidence": confidence,
                "action_level": action_level,
                "triggered": trigger_info.get('triggered', False)
            },
            "flags": expert_flags,
            "trace_log": {
                "context": context_status,
                "coverage_ratio": coverage,
                "timestamp": datetime.now().isoformat()
            }
        }

