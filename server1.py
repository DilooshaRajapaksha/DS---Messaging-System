"""
server1.py — PRIMARY Server
============================
MERGED: Your replication/quorum code + teammate's persistent
storage, /sync, peer health monitoring.

Run with:
    uvicorn server1:app --host 0.0.0.0 --port 8001 --reload
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time, sys, os, json, threading
import requests

sys.path.append(os.path.dirname(__file__))
from storage import Message, MessageStore
from replication import ReplicationConfig, ReplicationManager
from consistency import QuorumConfig

# ── Setup ─────────────────────────────────────────────────────────────────
app       = FastAPI(title="Server 1 - PRIMARY")
SERVER_ID = "server1"
FILE_NAME = "server1_messages.json"

PEERS = {
    "server2": "http://127.0.0.1:8002",
    "server3": "http://127.0.0.1:8003"
}

server_status = {"server2": "unknown", "server3": "unknown"}

local_store = MessageStore(SERVER_ID)
config      = ReplicationConfig()
replicator  = ReplicationManager(local_store, config, SERVER_ID)
quorum_cfg  = QuorumConfig(total_servers=3)

replicator.start_background_retry()

# ── Request Models ────────────────────────────────────────────────────────
class SendMessageRequest(BaseModel):
    sender:    str
    recipient: str
    content:   str

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

# ── Disk persistence (teammate's idea) ────────────────────────────────────
def persist_to_disk():
    """Save messages to JSON so they survive server restarts."""
    try:
        with open(FILE_NAME, "w") as f:
            json.dump([m.to_dict() for m in local_store.get_all()], f, indent=2)
    except Exception as e:
        print(f"[Server1] Disk error: {e}")

# ── Background peer health monitor (teammate's idea) ──────────────────────
def check_servers():
    """Check backup heartbeats every 3 seconds."""
    while True:
        for name, url in PEERS.items():
            try:
                r = requests.get(f"{url}/heartbeat", timeout=2)
                server_status[name] = "alive" if r.status_code == 200 else "failed"
            except:
                server_status[name] = "failed"
        time.sleep(3)

threading.Thread(target=check_servers, daemon=True).start()

# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {"message": "Server 1 is running", "role": "PRIMARY"}

@app.get("/heartbeat")
def heartbeat():
    """Teammate's original — kept exactly as is."""
    return {"status": "alive", "server": "server1"}

@app.get("/status")
def status():
    """Your quorum/store status + teammate's peer health."""
    return {
        "server":      SERVER_ID,
        "role":        "primary",
        "peer_health": server_status,
        "store":       local_store.summary(),
        "replication": replicator.status(),
        "quorum": {
            "total_servers": quorum_cfg.total_servers,
            "write_quorum":  quorum_cfg.write_quorum,
            "read_quorum":   quorum_cfg.read_quorum,
        },
    }

@app.post("/send")
def send_message(request: SendMessageRequest):
    """Client sends message → quorum write → replicate to backups."""
    msg = Message(
        sender=    request.sender,
        recipient= request.recipient,
        content=   request.content,
        timestamp= time.time()
    )
    results       = replicator.replicate(msg)
    confirmations = 1 + sum(1 for ok in results.values() if ok)
    persist_to_disk()

    if not quorum_cfg.is_write_quorum_met(confirmations):
        raise HTTPException(status_code=503, detail={
            "error":         "Quorum not met",
            "confirmations": confirmations,
            "quorum_needed": quorum_cfg.write_quorum,
        })

    return {
        "success":        True,
        "message_id":     msg.message_id,
        "confirmations":  confirmations,
        "quorum_met":     True,
        "replicated_to":  [SERVER_ID] + [s for s, ok in results.items() if ok],
        "failed_servers": [s for s, ok in results.items() if not ok],
        "timestamp":      msg.timestamp,
    }

@app.post("/replicate")
def receive_replicated_message(request: ReplicateRequest):
    """Receive a replicated copy from primary."""
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

@app.get("/messages")
def get_messages(recipient: str):
    """Read messages for a recipient."""
    msgs = local_store.get_by_recipient(recipient)
    return {"recipient": recipient, "count": len(msgs), "messages": [m.to_dict() for m in msgs]}

@app.get("/sync")
def sync_messages():
    """Teammate's /sync — backups call this to recover all messages after rejoining."""
    return {"messages": [m.to_dict() for m in local_store.get_all()]}