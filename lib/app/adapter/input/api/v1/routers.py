from fastapi import APIRouter
from lib.app.adapter.input.api.v1.controllers import part_controller, match_controller

api_router = APIRouter()
api_router.include_router(part_controller.router, prefix="/parts", tags=["Parts"])
api_router.include_router(match_controller.router, prefix="/matches", tags=["Matches"])
