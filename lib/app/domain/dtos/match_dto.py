from pydantic import BaseModel

class MatchDTO(BaseModel):
    source: str
    target: str
    match_type: str
