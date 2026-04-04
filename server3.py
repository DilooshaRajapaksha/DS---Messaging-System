from fastapi import FastAPI
from pydantic import BaseModel
from storage import load_messages, save_messages
import requests

app = FastAPI()

FILE_NAME = "server3_messages.json"
LEADER_URL = "http://127.0.0.1:8001"

messages = load_messages(FILE_NAME)

class Message(BaseModel):
    sender: str
    receiver: str
    content: str
    timestamp: float

@app.get("/")
def home():
    return {"message": "Server 3 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server3"}

@app.get("/messages")
def get_messages():
    return {"messages": messages}

@app.post("/replicate")
def replicate_message(message: Message):
    msg = message.dict()
    messages.append(msg)
    save_messages(FILE_NAME, messages)
    return {"message": "Message replicated to server3"}

@app.post("/recover")
def recover():
    global messages
    try:
        response = requests.get(f"{LEADER_URL}/sync", timeout=5)
        if response.status_code == 200:
            messages = response.json()["messages"]
            save_messages(FILE_NAME, messages)
            return {
                "message": "Recovery successful",
                "total_messages": len(messages)
            }
        return {"message": "Leader sync failed"}
    except Exception as e:
        return {"message": "Recovery failed", "error": str(e)}