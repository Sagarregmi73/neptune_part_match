from pydantic import BaseModel
from typing import Dict

class PartNumberDTO(BaseModel):
    part_number: str
    specs: Dict[str, str]  # dynamic specs
    notes: Dict[str, str] = {}  # dynamic notes
