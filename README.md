# Ads Manager Agent 🤖

A powerful AI-driven application for analyzing advertising campaigns. It combines a robust data dashboard with intelligent agents that perform deep-dive analysis on campaign performance, search terms, and demographics.

## ✨ Features

- **Dashboard**: Interactive data table with sorting, filtering, and date range selection.
- **Campaign Details**: Deep dive into specific campaigns with lazy-loaded tables for Products, Search Terms, Locations, etc.
- **AI Agents**:
  - **Main Agent**: Orchetsrates tasks and monitors anomalies (uses "Pro" reasoning model).
  - **Sub Agents**: Specialized agents for Pmax and Search campaigns (uses "Flash" speed model) to generate detailed reports.
- **Performance**: Optimized SQLite database with indexes and virtualized frontend scrolling for handling large datasets (18k+ products).

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

# Install dependencies
pip install -r requirements.txt
# (Note: manual install if requirements.txt missing: fastapi uvicorn pandas langchain-openai langgraph python-dotenv)
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
SUB_MAIN_MODEL_NAM=gemini-1.5-flash    # High speed for data analysis (Note variable name spelling)
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
│   ├── agent_service.py   # AI Logic (Main & Sub Agents)
│   ├── main.py           # FastAPI Routes
│   ├── .env              # Configuration
│   └── ...
├── frontend/
│   ├── src/
│   │   ├── components/   # Dashboard, CampaignDetail, etc.
│   │   └── ...
│   └── ...
└── ads_data.sqlite       # Database (Auto-generated/Indexed)
```

## ⚠️ Notes
- The database is automatically optimizing with indexes on `campaign` and `date` columns for performance.
- If you clone this repo, you **must** run `npm install` in the frontend directory again.
