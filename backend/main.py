from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
import sys
import os

# Add current directory to path to allow import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_service import AgentService

app = FastAPI()

# Allow CORS for React Frontend (default port 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

agent = AgentService()

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str
    messages: List[Message] = []

class PreferenceUpdateRequest(BaseModel):
    table_name: str
    item_identifier: str
    is_pinned: Optional[int] = None
    display_order: Optional[int] = None

class ResetPreferenceRequest(BaseModel):
    table_name: str

@app.get("/")
def read_root():
    return {"status": "Ads Manager API is running"}

@app.post("/api/scan")
def scan_campaigns():
    # Scan is still synchronous for now, but could be streamed too
    # For simplicity, we keep it as is or redirect to chat
    return {"report": "Please use the chat interface to scan."}

@app.post("/api/chat")
async def chat_with_agent(req: ChatRequest):
    return StreamingResponse(agent.chat_stream(req.message, req.messages), media_type="text/plain")

@app.get("/api/tables")
def get_tables():
    return {"tables": agent.get_tables()}

@app.get("/api/tables/{table_name}")
def get_table_data(table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    return agent.get_table_data(table_name, start_date, end_date)

@app.get("/api/campaigns/{campaign_name}/details")
def get_campaign_details(campaign_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    return agent.get_campaign_details(campaign_name, start_date, end_date)

@app.post("/api/preferences")
def update_preference(req: PreferenceUpdateRequest):
    response = agent.update_preference(
        table_name=req.table_name,
        item_identifier=req.item_identifier,
        is_pinned=req.is_pinned,
        display_order=req.display_order
    )
    return response

@app.post("/api/preferences/reset")
def reset_preferences(req: ResetPreferenceRequest):
    response = agent.reset_preferences(req.table_name)
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
