from fastapi import APIRouter, HTTPException, Depends
from typing import List
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.domain.entities.match import Match
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.core.utils.container import get_match_usecase

router = APIRouter()

@router.post("/", response_model=MatchDTO)
async def create_match(match_dto: MatchDTO, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    return await usecase.create_match(Match(**match_dto.dict()))

@router.get("/{source}/{target}", response_model=MatchDTO)
async def get_match(source: str, target: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    match = await usecase.get_match(source, target)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    return match

@router.put("/{source}/{target}", response_model=MatchDTO)
async def update_match(source: str, target: str, match_dto: MatchDTO, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    return await usecase.update_match(Match(**match_dto.dict()))

@router.delete("/{source}/{target}")
async def delete_match(source: str, target: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    success = await usecase.delete_match(source, target)
    return {"success": success}

@router.get("/", response_model=List[MatchDTO])
async def list_matches(usecase: MatchPartUseCase = Depends(get_match_usecase)):
    return await usecase.list_matches()

@router.get("/search/{part_number}")
async def search_matches(part_number: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    result = await usecase.get_matches_for_part(part_number)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result
