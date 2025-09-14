from dotenv import load_dotenv
load_dotenv("local.env")  # must be first
import os
from fastapi import FastAPI
from lib.app.adapter.input.api.v1.routers import api_router

app = FastAPI(title="PartNumber Matching API")
app.include_router(api_router)

# Quick debug print to verify environment variables
print("Loaded environment variables:")
print("NEPTUNE_ENDPOINT =", os.getenv("NEPTUNE_ENDPOINT"))
print("NEPTUNE_IAM_ROLE_ARN =", os.getenv("NEPTUNE_IAM_ROLE_ARN"))
print("AWS_REGION =", os.getenv("AWS_REGION"))