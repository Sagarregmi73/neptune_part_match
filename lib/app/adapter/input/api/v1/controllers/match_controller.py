from fastapi import APIRouter, HTTPException
from typing import List
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.domain.entities.match import Match
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.core.utils.container import get_match_usecase

router = APIRouter()
usecase = get_match_usecase()

@router.post("/", response_model=MatchDTO)
def create_match(match_dto: MatchDTO):
    return MatchDTO(**vars(usecase.create_match(Match(match_dto.source, match_dto.target, match_dto.match_type))))

@router.get("/{source}/{target}", response_model=MatchDTO)
def get_match(source: str, target: str):
    match = usecase.get_match(source, target)
    if not match:
        raise HTTPException(status_code=404, detail="Match not found")
    return MatchDTO(**vars(match))

@router.put("/{source}/{target}", response_model=MatchDTO)
def update_match(source: str, target: str, match_dto: MatchDTO):
    return MatchDTO(**vars(usecase.update_match(Match(source, target, match_dto.match_type))))

@router.delete("/{source}/{target}")
def delete_match(source: str, target: str):
    return {"success": usecase.delete_match(source, target)}

@router.get("/", response_model=List[MatchDTO])
def list_matches():
    return [MatchDTO(**vars(m)) for m in usecase.list_matches()]

@router.get("/search/{part_number}", response_model=List[MatchDTO])
def search_matches(part_number: str):
    matches = usecase.get_matches_for_part(part_number)
    return [MatchDTO(**vars(m)) for m in matches]
