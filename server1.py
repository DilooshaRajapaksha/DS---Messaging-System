"""
server1.py — PRIMARY Server
============================
Part 4 of Data Replication & Consistency module.

Server1 is the PRIMARY server:
- All client WRITES come here first
- It runs quorum_write() to ensure safety  (Part 3)
- It replicates messages to Server2 & 3    (Part 2)
- Stores messages locally                  (Part 1)

Run this server with:
    uvicorn server1:app --host 0.0.0.0 --port 8001 --reload
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import time
import sys
import os

# ── Import our modules (Parts 1, 2, 3) ───────
sys.path.append(os.path.dirname(__file__))
from storage import Message, MessageStore
from replication import ReplicationConfig, ReplicationManager
from consistency import QuorumConfig, ConsistencyManager

# ─────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────

app       = FastAPI(title="Server 1 - PRIMARY")
SERVER_ID = "server1"

# Part 1 — this server's local store
local_store = MessageStore(SERVER_ID)

# Part 2 — replication engine (pushes copies to server2 & server3)
config      = ReplicationConfig()
replicator  = ReplicationManager(local_store, config, SERVER_ID)

# Part 3 — consistency manager (quorum + dedup)
# Note: for quorum checks, we only have access to local store here.
# In a full distributed setup, stores would be shared via a DB.
# For now, quorum is enforced through replication confirmations.
quorum_cfg  = QuorumConfig(total_servers=3)

# Start background retry (automatically retry failed replications every 5s)
replicator.start_background_retry()


# ─────────────────────────────────────────────
# Request / Response Models
# ─────────────────────────────────────────────

class SendMessageRequest(BaseModel):
    """What a client sends when posting a new message."""
    sender:    str
    recipient: str
    content:   str

class ReplicateRequest(BaseModel):
    """What the primary sends to backups when replicating."""
    message_id:         str
    sender:             str
    recipient:          str
    content:            str
    timestamp:          float
    version:            int   = 1
    replication_status: str   = "pending"
    replicated_to:      list  = []
    status:             str   = "stored"


# ─────────────────────────────────────────────
# Original endpoints (kept from teammate's code)
# ─────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "Server 1 is running", "role": "PRIMARY"}

@app.get("/heartbeat")
def heartbeat():
    
    return {"status": "alive", "server": SERVER_ID}


# ─────────────────────────────────────────────
# New endpoints (Part 4 additions)
# ─────────────────────────────────────────────

@app.post("/send")
def send_message(request: SendMessageRequest):
    """
    CLIENT → sends a new message through this primary server.
    """
    # Step 1: Create message
    msg = Message(
        sender=    request.sender,
        recipient= request.recipient,
        content=   request.content,
        timestamp= time.time()
    )

    # Step 2: Replicate (saves locally + pushes to backups)
    results = replicator.replicate(msg)

    # Count confirmations (primary + successful backups)
    confirmations = 1 + sum(1 for ok in results.values() if ok)

    # Check quorum
    if not quorum_cfg.is_write_quorum_met(confirmations):
        raise HTTPException(
            status_code=503,
            detail={
                "error":          "Quorum not met — not enough servers available",
                "confirmations":  confirmations,
                "quorum_needed":  quorum_cfg.write_quorum,
                "message_id":     msg.message_id,
            }
        )

    return {
        "success":        True,
        "message_id":     msg.message_id,
        "confirmations":  confirmations,
        "quorum_met":     True,
        "replicated_to":  [SERVER_ID] + [sid for sid, ok in results.items() if ok],
        "failed_servers": [sid for sid, ok in results.items() if not ok],
        "timestamp":      msg.timestamp,
    }


@app.post("/replicate")
def receive_replicated_message(request: ReplicateRequest):
    """
    PRIMARY → sends a copy of a message to this server (backup).
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
        "saved":   saved,           # False means it was a duplicate → skipped
        "server":  SERVER_ID,
    }


@app.get("/messages")
def get_messages(recipient: str):
    """
    CLIENT → retrieves all messages for a specific recipient.

    """
    messages = local_store.get_by_recipient(recipient)

    if not messages:
        return {
            "recipient": recipient,
            "count":     0,
            "messages":  [],
        }

    return {
        "recipient": recipient,
        "count":     len(messages),
        "messages":  [m.to_dict() for m in messages],
    }


@app.get("/status")
def status():
    
    return {
        "server":       SERVER_ID,
        "role":         "primary",
        "store":        local_store.summary(),
        "replication":  replicator.status(),
        "quorum": {
            "total_servers": quorum_cfg.total_servers,
            "write_quorum":  quorum_cfg.write_quorum,
            "read_quorum":   quorum_cfg.read_quorum,
        },
    }