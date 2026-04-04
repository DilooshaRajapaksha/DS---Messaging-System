"""
server2.py — BACKUP Server
============================
MERGED: Your replication/quorum code + teammate's persistent
storage and /recover endpoint.

Run with:
    uvicorn server2:app --host 0.0.0.0 --port 8002 --reload
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys, os, json
import requests

sys.path.append(os.path.dirname(__file__))
from storage import Message, MessageStore

app = FastAPI(title="Server 2 - BACKUP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Setup ─────────────────────────────────────────────────────────────────
SERVER_ID = "server2"
FILE_NAME = "server2_messages.json"
LEADER_URL = "http://127.0.0.1:8001"

local_store = MessageStore(SERVER_ID)

# ── Request Model ─────────────────────────────────────────────────────────
class ReplicateRequest(BaseModel):
    message_id:         str
    sender:             str
    recipient:          str
    content:            str
    timestamp:          float
    version:            int  = 1
    replication_status: str  = "pending"
    replicated_to:      list = []
    status:             str  = "stored"

# ── Disk persistence ──────────────────────────────────────────────────────
def persist_to_disk():
    try:
        with open(FILE_NAME, "w") as f:
            json.dump([m.to_dict() for m in local_store.get_all()], f, indent=2)
    except Exception as e:
        print(f"[Server2] Disk error: {e}")

# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "Server 2 is running", "role": "BACKUP"}

@app.get("/heartbeat")
def heartbeat():
    """Teammate's original — kept exactly as is."""
    return {"status": "alive", "server": "server2"}

@app.get("/status")
def status():
    return {"server": SERVER_ID, "role": "backup", "store": local_store.summary()}

@app.post("/replicate")
def receive_replicated_message(request: ReplicateRequest):
    """Receive a copy from primary and save it."""
    msg = Message(
        message_id= request.message_id,
        sender=     request.sender,
        recipient=  request.recipient,
        content=    request.content,
        timestamp=  request.timestamp,
        version=    request.version,
    )
    saved = local_store.save(msg)
    if saved:
        persist_to_disk()
    return {"success": True, "saved": saved, "server": SERVER_ID}

@app.post("/send")
def send_not_allowed():
    """Backups reject direct writes — redirect to primary."""
    raise HTTPException(status_code=403, detail={
        "error":          "This is a backup — send messages to the primary",
        "primary_server": "http://localhost:8001/send",
    })

@app.get("/messages")
def get_messages(recipient: str):
    """Read messages for a recipient from this backup."""
    msgs = local_store.get_by_recipient(recipient)
    return {"recipient": recipient, "server": SERVER_ID, "count": len(msgs), "messages": [m.to_dict() for m in msgs]}

@app.post("/recover")
def recover():
    """
    Teammate's /recover endpoint.
    When this backup comes back online after a crash, it calls
    primary's /sync to pull all messages it missed.
    """
    global local_store
    try:
        response = requests.get(f"{LEADER_URL}/sync", timeout=5)
        if response.status_code == 200:
            all_messages = response.json()["messages"]
            recovered = 0
            for item in all_messages:
                msg = Message.from_dict(item)
                if local_store.save(msg):
                    recovered += 1
            persist_to_disk()
            return {
                "message":          "Recovery successful",
                "total_messages":   local_store.count(),
                "newly_recovered":  recovered,
            }
        return {"message": "Leader sync failed"}
    except Exception as e:
        return {"message": "Recovery failed", "error": str(e)}