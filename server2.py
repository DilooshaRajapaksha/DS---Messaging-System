from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Server 2 is running"}