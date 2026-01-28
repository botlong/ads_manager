from fastapi import FastAPI, Depends, HTTPException, status, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Optional, List
import sys
import os

# Add current directory to path to allow import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_service import AgentService
import auth

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

# --- Auth Models & Dependency ---
class LoginRequest(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    username: str
    role: str

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    username = auth.verify_token(token)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return username

# --- Data Models ---
class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str
    messages: List[Message] = []
    selectedTables: Optional[List[str]] = []

class PreferenceUpdateRequest(BaseModel):
    table_name: str
    item_identifier: str
    is_pinned: Optional[int] = None
    display_order: Optional[int] = None

class ResetPreferenceRequest(BaseModel):
    table_name: str

class CustomRuleRequest(BaseModel):
    table_name: str
    rule_prompt: str

# --- Public Endpoints ---

@app.get("/")
def read_root():
    return {"status": "Ads Manager API is running"}

@app.post("/api/login", response_model=Token)
def login(req: LoginRequest):
    user = auth.get_user(req.username)
    if not user or not auth.verify_password(user['password_hash'], req.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = auth.generate_token(req.username)
    return {
        "access_token": token,
        "token_type": "bearer",
        "username": user['username'],
        "role": user['role']
    }

@app.post("/api/logout")
def logout(token: str = Depends(oauth2_scheme)):
    auth.invalidate_token(token)
    return {"status": "success", "message": "Logged out"}

# --- Protected Endpoints ---

@app.post("/api/scan")
def scan_campaigns(current_user: str = Depends(get_current_user)):
    return {"report": "Please use the chat interface to scan."}

@app.post("/api/chat")
async def chat_with_agent(req: ChatRequest, current_user: str = Depends(get_current_user)):
    return StreamingResponse(agent.chat_stream(req.message, req.messages, req.selectedTables), media_type="text/plain")

@app.get("/api/tables")
def get_tables(current_user: str = Depends(get_current_user)):
    return {"tables": agent.get_tables()}

@app.get("/api/tables/{table_name}")
def get_table_data(table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, current_user: str = Depends(get_current_user)):
    return agent.get_table_data(table_name, start_date, end_date)

@app.get("/api/campaigns/{campaign_name}/details")
def get_campaign_details(campaign_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, current_user: str = Depends(get_current_user)):
    return agent.get_campaign_details(campaign_name, start_date, end_date)

@app.get("/api/campaigns/{campaign_name}/anomaly-details")
def get_campaign_anomaly_details(campaign_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, current_user: str = Depends(get_current_user)):
    """Get ONLY anomaly data for a campaign - filtered by hard rules"""
    return agent.get_campaign_anomaly_details(campaign_name, start_date, end_date)

@app.get("/api/anomalies/campaign")
async def get_campaign_anomalies(target_date: str = None, current_user: str = Depends(get_current_user)):
    anomalies = agent.get_campaign_anomalies(target_date=target_date)
    return anomalies

@app.get("/api/anomalies/campaign/date-range")
async def get_campaign_anomalies_date_range(current_user: str = Depends(get_current_user)):
    """Get the analyzable date range for campaign anomalies"""
    return agent.get_campaign_analyzable_date_range()

@app.get("/api/anomalies/product/date-range")
async def get_product_anomalies_date_range(current_user: str = Depends(get_current_user)):
    """Get the analyzable date range for product anomalies"""
    return agent.get_product_analyzable_date_range()

@app.get("/api/anomalies/product")
async def get_product_anomalies(target_date: str = None, current_user: str = Depends(get_current_user)):
    # Note: Removed pagination logic from main.py to match previous state if it wasn't fully implemented in agent_service
    # If pagination is needed, it should be handled here or in agent_service. 
    # For now, returning full list as per previous working state.
    anomalies = agent.get_product_anomalies(target_date=target_date)
    return anomalies

@app.post("/api/preferences")
def update_preference(req: PreferenceUpdateRequest, current_user: str = Depends(get_current_user)):
    response = agent.update_preference(
        table_name=req.table_name,
        item_identifier=req.item_identifier,
        is_pinned=req.is_pinned,
        display_order=req.display_order
    )
    return response

@app.post("/api/preferences/reset")
def reset_preferences(req: ResetPreferenceRequest, current_user: str = Depends(get_current_user)):
    response = agent.reset_preferences(req.table_name)
    return response

@app.post("/api/agent-rules")
def save_custom_rule(req: CustomRuleRequest, current_user: str = Depends(get_current_user)):
    """Save a custom rule to the database"""
    return agent.save_custom_rule(req.table_name, req.rule_prompt)

@app.get("/api/agent-rules/{table_name}")
def get_custom_rules(table_name: str, current_user: str = Depends(get_current_user)):
    """Get saved custom rules for a table"""
    return agent.get_custom_rules(table_name)

@app.get("/api/agent-prompts/{table_name}")
def get_agent_default_prompt(table_name: str, current_user: str = Depends(get_current_user)):
    """Get the default prompt/rules for a specific agent"""
    return agent.get_agent_default_prompt(table_name)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
