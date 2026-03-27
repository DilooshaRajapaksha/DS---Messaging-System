from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Server 1 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server1"}