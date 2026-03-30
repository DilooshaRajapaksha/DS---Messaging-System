"""
server3.py — BACKUP Server
============================
Part 4 of Data Replication & Consistency module.

Server3 is a BACKUP server (identical role to Server2):
- Receives replicated copies from Server1 (primary)
- Clients can READ messages from here
- Does NOT accept direct client writes (redirects to primary)

Run this server with:
    uvicorn server3:app --host 0.0.0.0 --port 8003 --reload
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sys
import os

sys.path.append(os.path.dirname(__file__))
from storage import Message, MessageStore

# ─────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────

app       = FastAPI(title="Server 3 - BACKUP")
SERVER_ID = "server3"

# Part 1 — this server's local store
local_store = MessageStore(SERVER_ID)


# ─────────────────────────────────────────────
# Request Model
# ─────────────────────────────────────────────

class ReplicateRequest(BaseModel):
    """What the primary sends to this backup when replicating."""
    message_id:         str
    sender:             str
    recipient:          str
    content:            str
    timestamp:          float
    version:            int  = 1
    replication_status: str  = "pending"
    replicated_to:      list = []
    status:             str  = "stored"


# ─────────────────────────────────────────────
# Original endpoints (kept from teammate's code)
# ─────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "Server 3 is running", "role": "BACKUP"}

@app.get("/heartbeat")
def heartbeat():
   
    return {"status": "alive", "server": SERVER_ID}


# ─────────────────────────────────────────────
# New endpoints (Part 4 additions)
# ─────────────────────────────────────────────

@app.post("/replicate")
def receive_replicated_message(request: ReplicateRequest):
    """
    PRIMARY → sends a copy of a message to this backup server.

    """
    msg = Message(
        message_id= request.message_id,
        sender=     request.sender,
        recipient=  request.recipient,
        content=    request.content,
        timestamp=  request.timestamp,
        version=    request.version,
    )

    saved = local_store.save(msg)

    return {
        "success": True,
        "saved":   saved,     # False = duplicate, was skipped
        "server":  SERVER_ID,
    }


@app.post("/send")
def send_not_allowed():
    
    raise HTTPException(
        status_code=403,
        detail={
            "error":           "This is a backup server — send messages to the primary",
            "primary_server":  "http://localhost:8001/send",
        }
    )


@app.get("/messages")
def get_messages(recipient: str):
    """
    CLIENT → retrieves messages for a recipient from this backup.

    """
    messages = local_store.get_by_recipient(recipient)

    return {
        "recipient":   recipient,
        "server":      SERVER_ID,
        "count":       len(messages),
        "messages":    [m.to_dict() for m in messages],
    }


@app.get("/status")
def status():
    """
    Shows the current health of this backup server.

    """
    return {
        "server": SERVER_ID,
        "role":   "backup",
        "store":  local_store.summary(),
    }