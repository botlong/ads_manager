from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import pandas as pd
import sqlite3
import sys
import io

# Redirect stdout to file (encoding verified)
sys.stdout = io.TextIOWrapper(open('llm_agent_output.txt', 'wb'), encoding='utf-8')

DB_FILE = 'ads_data.sqlite'
MODEL_NAME = "qwen3:8b-q8_0"
BASE_URL = "http://localhost:11434/v1"

class LLMMasterAgent:
    def __init__(self):
        print(f"Initializing ChatOpenAI with model={MODEL_NAME} at {BASE_URL}")
        self.llm = ChatOpenAI(
            model=MODEL_NAME,
            base_url=BASE_URL,
            api_key="none",
            temperature=0
        )
        # Using the exact prompt provided by the user
        self.prompt = ChatPromptTemplate.from_template("""# Role
你是一个 Google Ads 账户监控主理人 (Main Agent)。你的职责是读取 Campaign 报表中的预计算指标，快速发现异常，并在用户询问时调用子 Agent。

# Input Data Structure (数据理解)
你将处理一份包含预先对比数据的表格 (下方的 # Data 部分)。请重点关注以下列名：
- **核心指标 (本期):** `ROAS`, `Cost / conv.` (即 CPA), `Conversions`
- **对比指标 (上期):** `ROAS(Compare to)`, `Cost / conv. (Compare to)`
- **广告系列类型:** `Campaign type` (用于区分 PMax 或 Search)

# Operational Workflow (工作流)

## Phase 1: 快速扫描与报警 (默认模式)
当接收到数据时，请逐行扫描 Campaign，直接对比两列数据：

1. **异常判定逻辑 (直接读取):**
   - **ROAS 报警:** 当 `ROAS` < `ROAS(Compare to)` × 0.8 时 (即下降超过 20%)。
   - **CPA 报警:** 当 `Cost / conv.` > `Cost / conv. (Compare to)` × 1.25 时 (即上升超过 25%)。
   - *过滤条件:* 忽略 `Conversions` < 3 的广告系列 (避免数据量过小造成的误判)。同时忽略 `--`, `Total` 等汇总行。

2. **输出动作:**
   - **如果没有触警:** 回复 “✅ 本周数据扫描完成，所有 Campaign 的 ROAS 和 CPA 波动均在正常范围内。”
   - **如果触警:** 仅列出问题 Campaign，格式如下：
     > ⚠️ **监测报告：发现异常 Campaign**
     > **1. [Campaign Name]** (`[Campaign Type]`)
     >    - **ROAS:** [ROAS值] vs [ROAS(Compare to)] (下降 [计算百分比]%) 🔴
     >    - **CPA:** [Cost/conv.值] vs [Compare值] (上升 [计算百分比]%)
     > *(等待用户指令...)*

## Phase 2: 深度诊断路由 (仅在用户追问时触发)
(在此次运行中，请如果发现问题，在报告后直接模拟 Phase 2 的路由决策，说明你会呼叫哪个子 Agent)

# Data
{data}

请根据上述 Role, Workflow 和 Data，输出分析报告。请严格遵守判定逻辑。
""")

    def load_data(self):
        conn = sqlite3.connect(DB_FILE)
        # Select relevant columns
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
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Rename columns to match the prompt's expectation
        df = df.rename(columns={
            'campaign': 'Campaign Name',
            'campaign_type': 'Campaign type',
            'roas': 'ROAS',
            'roascompare_to': 'ROAS(Compare to)',
            'cost_conv': 'Cost / conv.',
            'cost_conv_compare_to': 'Cost / conv. (Compare to)',
            'conversions': 'Conversions'
        })
        return df

    def run(self):
        try:
            df = self.load_data()
            print(f"Loaded {len(df)} rows from DB.")
            
            # Format as Markdown table
            data_str = df.to_markdown(index=False)
            
            chain = self.prompt | self.llm | StrOutputParser()
            
            print("Invoking LLM...")
            result = chain.invoke({"data": data_str})
            
            print("\n=== Agent Output ===\n")
            print(result)
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    agent = LLMMasterAgent()
    agent.run()
