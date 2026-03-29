from fastapi import FastAPI
from storage import load_messages

app = FastAPI()

FILE_NAME = "server3_messages.json"
messages = load_messages(FILE_NAME)

@app.get("/")
def home():
    return {"message": "Server 3 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server3"}

@app.get("/messages")
def get_messages():
    return {"messages": messages}