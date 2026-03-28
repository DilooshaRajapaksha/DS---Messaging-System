from fastapi import FastAPI
import requests
import threading
import time

app = FastAPI()

PEERS = {
    "server2": "http://127.0.0.1:8002/heartbeat",
    "server3": "http://127.0.0.1:8003/heartbeat"
}

server_status = {
    "server2": "unknown",
    "server3": "unknown"
}

@app.get("/")
def home():
    return {"message": "Server 1 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server1"}

@app.get("/status")
def status():
    return server_status

def check_servers():
    while True:
        for name, url in PEERS.items():
            try:
                response = requests.get(url, timeout=2)
                if response.status_code == 200:
                    server_status[name] = "alive"
                else:
                    server_status[name] = "failed"
            except:
                server_status[name] = "failed"
        time.sleep(3)

thread = threading.Thread(target=check_servers, daemon=True)
thread.start()