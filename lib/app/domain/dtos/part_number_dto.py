# lib/app/domain/dtos/part_number_dto.py

from pydantic import BaseModel

class PartNumberDTO(BaseModel):
    part_number: str
    specs: str
    notes: str = ""
