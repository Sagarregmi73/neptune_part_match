from lib.app.domain.entities.match import Match
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository


class MatchPartUseCase:
    def __init__(self, repository: NeptuneRepository):
        self.repository = repository

    def create_match(self, match: Match) -> MatchDTO:
        saved = self.repository.save_match(match)
        return MatchDTO(**saved.__dict__)

    def get_match(self, source: str, target: str) -> MatchDTO | None:
        match = self.repository.get_match(source, target)
        return MatchDTO(**match.__dict__) if match else None

    def update_match(self, match: Match) -> MatchDTO:
        updated = self.repository.update_match(match)
        return MatchDTO(**updated.__dict__)

    def delete_match(self, source: str, target: str) -> bool:
        return self.repository.delete_match(source, target)

    def list_matches(self) -> list[MatchDTO]:
        matches = self.repository.list_matches()
        return [MatchDTO(**m.__dict__) for m in matches]

    def get_matches_for_part(self, part_number: str) -> list[MatchDTO]:
        matches = self.repository.get_matches_for_part(part_number)
        return [MatchDTO(**m.__dict__) for m in matches]
