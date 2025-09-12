from fastapi import FastAPI
from lib.app.adapter.input.api.v1.routers import api_router

app = FastAPI(title="PartNumber Matching API")
app.include_router(api_router)
