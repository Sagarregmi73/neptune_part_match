# lib/main.py
from fastapi import FastAPI
from lib.app.adapter.input.api.v1.routers import api_router
from lib.core.logging import logger

app = FastAPI(title="Part Number Matching API")

# Include API routes
app.include_router(api_router)

@app.get("/")
def root():
    return {"message": "Part Number Matching API is running."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
