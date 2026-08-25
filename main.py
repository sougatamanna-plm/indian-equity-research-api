from fastapi import FastAPI
from datetime import datetime

app = FastAPI(
    title="Indian Equity Research API",
    description="Free NSE/BSE research data gateway",
    version="0.1.0"
)


@app.get("/")
def root():
    return {
        "service": "Indian Equity Research API",
        "status": "online",
        "version": "0.1.0"
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Indian Equity Research API",
        "timestamp": datetime.utcnow().isoformat()
    }
