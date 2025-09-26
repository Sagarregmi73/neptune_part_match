# lib/app/domain/dtos/part_number_dto.py
from pydantic import BaseModel

class PartNumberDTO(BaseModel):
    id: str = None
    part_number: str
    spec1: str
    spec2: str
    spec3: str
    spec4: str
    spec5: str
    note1: str = ""
    note2: str = ""
    note3: str = ""
