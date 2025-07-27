from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from .api.endpoints import quiz
from ..config import SECRET_TOKEN

app = FastAPI(title="Religious Questions Bot API")

api_key_header = APIKeyHeader(name="Authorization", auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == SECRET_TOKEN:
        return api_key_header
    else:
        raise HTTPException(status_code=403, detail="Could not validate credentials")

app.include_router(quiz.router, prefix="/api", dependencies=[Depends(get_api_key)])

@app.get("/")
async def root():
    return {"message": "Welcome to the Religious Questions Bot API"}
