# lib/app/adapter/input/api/v1/controllers/match_controller.py

from fastapi import APIRouter, HTTPException, Depends
from typing import List
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.domain.entities.match import Match
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.core.utils.container import get_match_usecase

router = APIRouter()

# Create a match
@router.post("/", response_model=MatchDTO)
def create_match(match_dto: MatchDTO, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    return usecase.create_match(Match(**match_dto.dict()))

# Get a match
@router.get("/{source}/{target}", response_model=MatchDTO)
def get_match(source: str, target: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    match = usecase.get_match(source, target)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    return match

# Update a match
@router.put("/{source}/{target}", response_model=MatchDTO)
def update_match(source: str, target: str, match_dto: MatchDTO, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    if source != match_dto.source or target != match_dto.target:
        raise HTTPException(status_code=400, detail="Source/Target mismatch")
    return usecase.update_match(Match(**match_dto.dict()))

# Delete a match
@router.delete("/{source}/{target}")
def delete_match(source: str, target: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    success = usecase.delete_match(source, target)
    return {"success": success}

# List all matches
@router.get("/", response_model=List[MatchDTO])
def list_matches(usecase: MatchPartUseCase = Depends(get_match_usecase)):
    return usecase.list_matches()

# Search matches for a part
@router.get("/search/{part_number}")
def search_matches(part_number: str, usecase: MatchPartUseCase = Depends(get_match_usecase)):
    result = usecase.get_matches_for_part(part_number)
    if not result:
        raise HTTPException(status_code=404, detail=f"No matches found for part {part_number}")
    return result
