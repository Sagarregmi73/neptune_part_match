from pydantic import BaseModel

class PartNumberDTO(BaseModel):
    part_number: str
    specs: str
    notes: str = ""
