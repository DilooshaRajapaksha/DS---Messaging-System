from fastapi import FastAPI
from pydantic import BaseModel
from storage import load_messages, save_messages

app = FastAPI()

FILE_NAME = "server2_messages.json"
messages = load_messages(FILE_NAME)

class Message(BaseModel):
    sender: str
    receiver: str
    content: str
    timestamp: float

@app.get("/")
def home():
    return {"message": "Server 2 is running"}

@app.get("/heartbeat")
def heartbeat():
    return {"status": "alive", "server": "server2"}

@app.get("/messages")
def get_messages():
    return {"messages": messages}

@app.post("/replicate")
def replicate_message(message: Message):
    msg = message.dict()
    messages.append(msg)
    save_messages(FILE_NAME, messages)
    return {"message": "Message replicated to server2"}