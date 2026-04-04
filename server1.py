from fastapi import FastAPI
from pydantic import BaseModel
import requests
import threading
import time
from storage import load_messages, save_messages

app = FastAPI()

FILE_NAME = "server1_messages.json"

PEERS = {
    "server2": "http://127.0.0.1:8002",
    "server3": "http://127.0.0.1:8003"
}

server_status = {
    "server2": "unknown",
    "server3": "unknown"
}

messages = load_messages(FILE_NAME)

class Message(BaseModel):
    sender: str
    receiver: str
    content: str
    timestamp: float

@app.get("/")
def home():
    return {"message": "Server 1 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server1"}

@app.get("/status")
def status():
    return server_status

@app.get("/messages")
def get_messages():
    return {"messages": messages}

@app.get("/sync")
def sync_messages():
    return {"messages": messages}

@app.post("/send")
def send_message(message: Message):
    msg = message.dict()
    messages.append(msg)
    save_messages(FILE_NAME, messages)

    replication_result = {}

    for name, base_url in PEERS.items():
        try:
            response = requests.post(f"{base_url}/replicate", json=msg, timeout=2)
            if response.status_code == 200:
                replication_result[name] = "replicated"
                server_status[name] = "alive"
            else:
                replication_result[name] = "failed"
                server_status[name] = "failed"
        except:
            replication_result[name] = "failed"
            server_status[name] = "failed"

    return {
        "message": "Stored in leader and sent to backups",
        "data": msg,
        "replication": replication_result
    }



def check_servers():
    while True:
        for name, base_url in PEERS.items():
            try:
                response = requests.get(f"{base_url}/heartbeat", timeout=2)
                if response.status_code == 200:
                    server_status[name] = "alive"
                else:
                    server_status[name] = "failed"
            except:
                server_status[name] = "failed"
        time.sleep(3)

thread = threading.Thread(target=check_servers, daemon=True)
thread.start()