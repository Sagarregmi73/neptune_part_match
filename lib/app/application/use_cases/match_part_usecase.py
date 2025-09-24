from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.match import Match

class MatchPartUseCase:
    def __init__(self, repository: RepositoryInterface):
        self.repository = repository

    def create_match(self, match: Match) -> Match:
        return self.repository.create_match(match)

    def get_match(self, source: str, target: str):
        return self.repository.get_match(source, target)

    def update_match(self, match: Match) -> Match:
        return self.repository.update_match(match)

    def delete_match(self, source: str, target: str) -> bool:
        return self.repository.delete_match(source, target)

    def list_matches(self):
        return self.repository.list_matches()

    def get_matches_for_part(self, part_number: str):
        return self.repository.get_matches_for_part(part_number)
