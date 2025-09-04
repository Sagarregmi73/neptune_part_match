# lib/app/adapter/input/api/v1/controllers/match_controller.py
from fastapi import APIRouter, HTTPException
from typing import List
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.domain.entities.match import Match

router = APIRouter()

# Inject your repository here
from lib.core.utils.container import get_match_repository
usecase = MatchPartUseCase(get_match_repository())

@router.post("/", response_model=MatchDTO)
def create_match(match_dto: MatchDTO):
    match = Match(match_dto.source, match_dto.target, match_dto.match_type)
    created = usecase.create_match(match)
    return MatchDTO(**vars(created))

@router.get("/{source}/{target}", response_model=MatchDTO)
def get_match(source: str, target: str):
    match = usecase.get_match(source, target)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    return MatchDTO(**vars(match))

@router.put("/{source}/{target}", response_model=MatchDTO)
def update_match(source: str, target: str, match_dto: MatchDTO):
    match = Match(source, target, match_dto.match_type)
    updated = usecase.update_match(match)
    return MatchDTO(**vars(updated))

@router.delete("/{source}/{target}")
def delete_match(source: str, target: str):
    success = usecase.delete_match(source, target)
    return {"success": success}

@router.get("/", response_model=List[MatchDTO])
def list_matches():
    matches = usecase.list_matches()
    return [MatchDTO(**vars(m)) for m in matches]
