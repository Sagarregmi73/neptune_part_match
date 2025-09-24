
from lib.core.logging import logger

from fastapi import FastAPI
from lib.app.adapter.input.api.v1.routers import api_router

app = FastAPI(
    title="Part Matching API",
    description="API for managing parts and matches with Neptune DB",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.include_router(api_router, prefix="/api")

@app.get("/")
def root():
    return {"message": "Part Matching API is running!"}
