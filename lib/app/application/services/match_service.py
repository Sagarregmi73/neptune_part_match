# lib/app/application/services/match_service.py
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.services.match_logic import MatchLogic
from lib.app.domain.entities.match import Match
from lib.core.logging import logger

class MatchService:
    def __init__(self, repository: RepositoryInterface):
        self.repository = repository
        self.logic = MatchLogic()

    # Create a match with automatic type detection
    def create_match(self, source_specs: str, source_notes: str, target_specs: str, target_notes: str, source: str, target: str) -> Match:
        match_type = self.logic.determine_match(source_specs, source_notes, target_specs, target_notes)
        match = Match(source, target, match_type)
        self.repository.create_match(match)
        logger.info(f"Match created: {match}")
        return match

    def get_match(self, source: str, target: str) -> Match:
        return self.repository.get_match(source, target)

    def update_match(self, match: Match) -> Match:
        return self.repository.update_match(match)

    def delete_match(self, source: str, target: str) -> bool:
        return self.repository.delete_match(source, target)

    def list_matches(self):
        return self.repository.list_matches()
