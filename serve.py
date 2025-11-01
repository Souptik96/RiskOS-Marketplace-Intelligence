#!/usr/bin/env python3
"""In-process FastAPI facade around api.main for Hugging Face Spaces."""

import os
from typing import Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from api import main as core

api_app = FastAPI(title="DataWeaver In-Process API", version=core.app.version)
api_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ExecuteResponse(BaseModel):
    sql: str
    rows: list
    review: Dict


class ReviewResponse(BaseModel):
    sql: str
    review: Dict


class GeneratePayload(BaseModel):
    q: str


@api_app.get("/health")
def health() -> Dict[str, str]:
    return {"ok": "true", "provider": os.getenv("LLM_PROVIDER", "hf")}


@api_app.get("/execute", response_model=ExecuteResponse)
def execute(q: str):
    return core.execute(q)  # type: ignore[arg-type]


@api_app.post("/review", response_model=ReviewResponse)
def review(payload: GeneratePayload):
    return core.review(core.ReviewRequest(q=payload.q, sql=""))


@api_app.post("/nl2sql")
def nl2sql(payload: GeneratePayload):
    return core.nl2sql(core.GenRequest(q=payload.q))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api_app, host="0.0.0.0", port=int(os.getenv("PORT", "7861")))
