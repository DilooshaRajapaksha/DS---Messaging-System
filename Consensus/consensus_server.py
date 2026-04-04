from fastapi import FastAPI, Request, HTTPException
from Dilanka.raft_node import RaftNode, RaftState
from Dilanka.models import Message, MessageStore
from Time_sync import SynchronizedClock
import asyncio
import os
import sys

app = FastAPI()

# Configuration from environment or defaults
NODE_ID = os.getenv("NODE_ID", "server1")
PEERS = os.getenv("PEERS", "http://127.0.0.1:8002,http://127.0.0.1:8003").split(",")
PORT = int(os.getenv("PORT", 8001))

# Initialize components
store = MessageStore(NODE_ID)
clock = SynchronizedClock(NODE_ID)
raft = RaftNode(NODE_ID, PEERS)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(raft.run())

@app.post("/request_vote")
async def request_vote(request: Request):
    data = await request.json()
    return raft.handle_request_vote(
        data["term"], 
        data["candidate_id"], 
        data["last_log_index"], 
        data["last_log_term"]
    )

@app.post("/append_entries")
async def append_entries(request: Request):
    data = await request.json()
    result = raft.handle_append_entries(
        data["term"],
        data["leader_id"],
        data["prev_log_index"],
        data["prev_log_term"],
        data["entries"],
        data["leader_commit"]
    )
    
    # If successful, update our local store with committed entries
    if result["success"]:
        for i in range(raft.last_applied + 1, raft.commit_index + 1):
            entry = raft.log[i]
            msg = Message.from_dict(entry["message"])
            store.save(msg)
            raft.last_applied = i
            print(f"[{NODE_ID}] Applied message {msg.message_id[:8]} to store")
            
    return result

@app.post("/send_message")
async def send_message(request: Request):
    if raft.state != RaftState.LEADER:
        # Redirect or inform client about the leader
        raise HTTPException(status_code=400, detail="Not the leader")
    
    data = await request.json()
    # Create message with synchronized timestamp
    msg = Message(
        sender=data["sender"],
        recipient=data["recipient"],
        content=data["content"],
        timestamp=clock.get_current_timestamp()
    )
    
    # Append to log
    entry = {"term": raft.current_term, "message": msg.to_dict()}
    raft.log.append(entry)
    
    # Wait for commitment (simple polling for demo)
    idx = len(raft.log) - 1
    timeout = 5.0
    start_time = asyncio.get_event_loop().time()
    while raft.commit_index < idx:
        if asyncio.get_event_loop().time() - start_time > timeout:
            raise HTTPException(status_code=504, detail="Consensus timeout")
        await asyncio.sleep(0.1)
        
    # Apply to local store
    if raft.last_applied < idx:
        store.save(msg)
        raft.last_applied = idx
        
    return {"status": "committed", "message_id": msg.message_id}

@app.get("/messages")
async def get_messages():
    return [m.to_dict() for m in store.get_all()]

@app.get("/status")
async def get_status():
    return {
        "node_id": NODE_ID,
        "state": raft.state.name,
        "term": raft.current_term,
        "commit_index": raft.commit_index,
        "last_applied": raft.last_applied,
        "log_size": len(raft.log)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
