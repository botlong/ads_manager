# Ads Manager Agent 🤖

A powerful AI-driven application for analyzing advertising campaigns. It combines a robust data dashboard with intelligent agents that perform deep-dive analysis on campaign performance, search terms, and demographics.

## ✨ Features

### Dashboard & Analytics
- **Dashboard**: Interactive data table with sorting, filtering, and date range selection.
- **Campaign Details**: Deep dive into specific campaigns with lazy-loaded tables for Products, Search Terms, Locations, etc.
- **Resizable Tables**: Flexible column widths with persistent settings.

### Anomaly Detection
- **Anomaly Monitor**: Real-time detection of performance anomalies with sortable metrics (Conversions, ROAS, CPA, and their changes).
- **Product Monitor**: Product-level anomaly detection and monitoring integrated into the dashboard.
- **Custom Rules**: User-defined rules for anomaly thresholds and detection logic.

### AI Agents
- **Main Agent**: Orchestrates tasks and monitors anomalies (uses "Pro" reasoning model).
- **Sub Agents**: Specialized agents for Pmax and Search campaigns (uses "Flash" speed model) to generate detailed reports.
- **Pure LLM Analysis**: Direct LLM-based data interpretation with user-customizable prompts and rules.
- **Expert System**: Advanced rule-based analysis engine for campaign optimization.
- **Per-Agent Rule Editor**: Configure custom rules for each specialized agent.

### Performance
- Optimized SQLite database with indexes for handling large datasets (18k+ products).
- Virtualized frontend scrolling for smooth UI performance.
- Lazy-loaded data tables for efficient memory usage.

## 🛠️ Tech Stack

- **Backend**: Python (FastAPI), SQLite, LangChain, LangGraph.
- **Frontend**: React (Vite), functional components, CSS modules.
- **LLM**: Multi-model support (OpenAI-compatible API).

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Node.js 16+

### 1. Backend Setup

```bash
cd backend

# Create virtual environment (optional but recommended)
python -m venv venv
# Windows
venv\Scripts\activate
# Mac/Linux
source venv/bin/activate

# Install dependencies from root
pip install -r ../requirements.txt
```

### 2. Configuration (.env)

Create a `.env` file in the `backend/` directory:

```env
# Database Path (Auto-detected usually, but configurable if needed)
# DB_PATH=../ads_data.sqlite

# LLM Configuration
BASE_URL=http://localhost:11434/v1  # Or your API provider
API_KEY=your-api-key                # "not-needed" for local Ollama

# Model Selection
MAIN_MODEL_NAME=qwen2.5-72b-instruct   # High intelligence for routing
SUB_MAIN_MODEL_NAME=gemini-1.5-flash   # High speed for data analysis
```

### 3. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start Dev Server
npm run dev
```

### 4. Running the Application

1. **Start Backend**:
   ```bash
   cd backend
   python main.py
   # Server runs at http://localhost:8000
   ```

2. **Start Frontend**:
   ```bash
   cd frontend
   npm run dev
   # App runs at http://localhost:5173
   ```

## 🧩 Project Structure

```
ads_manager/
├── backend/
│   ├── main.py              # FastAPI Routes & API endpoints
│   ├── agent_service.py     # AI Logic (Main & Sub Agents)
│   ├── expert_system.py     # Rule-based analysis engine
│   ├── auth.py              # Authentication module
│   ├── init_prefs_db.py     # Preferences database initialization
│   └── .env                 # Configuration
├── frontend/
│   └── src/
│       └── components/
│           ├── Dashboard.jsx           # Main dashboard view
│           ├── CampaignDetail.jsx      # Campaign detail page
│           ├── AnomalyDashboard.jsx    # Anomaly monitoring panel
│           ├── ProductMonitor.jsx      # Product anomaly monitor
│           ├── AgentChat.jsx           # AI agent chat interface
│           ├── ResizableTable.jsx      # Flexible data table component
│           ├── CustomRuleEditor.jsx    # Custom rule configuration
│           ├── PerAgentRuleEditor.jsx  # Per-agent rule settings
│           └── Login.jsx               # Authentication page
├── ads_data.sqlite           # Database (Auto-generated/Indexed)
├── requirements.txt          # Python dependencies
└── import_ads_data.py        # Data import utility
```

## ⚠️ Notes

- The database is automatically optimized with indexes on `campaign` and `date` columns for performance.
- If you clone this repo, you **must** run `npm install` in the frontend directory.
- Custom analysis rules can be configured through the UI and are persisted per-agent.
