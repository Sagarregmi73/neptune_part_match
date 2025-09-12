# lib/app/application/services/match_service.py
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.services.match_logic import MatchLogic
from lib.app.domain.entities.match import Match
from lib.core.logging import logger

class MatchService:
    """
    Service layer for handling Match CRUD operations and business logic.
    """

    def __init__(self, repository: RepositoryInterface):
        self.repository = repository
        self.logic = MatchLogic()

    # ------------------- CREATE -------------------
    def create_match(self, source: str, target: str, source_specs: str, source_notes: str, 
                     target_specs: str, target_notes: str) -> Match:
        """
        Create a match and automatically determine match_type.
        """
        match_type = self.logic.determine_match(source_specs, source_notes, target_specs, target_notes)
        match = Match(source=source, target=target, match_type=match_type)
        self.repository.create_match(match)
        logger.info(f"Match created: {match}")
        return match

    # ------------------- READ ---------------------
    def get_match(self, source: str, target: str) -> Match:
        return self.repository.get_match(source, target)

    def list_matches(self):
        return self.repository.list_matches()

    # ------------------- UPDATE -------------------
    def update_match(self, match: Match) -> Match:
        updated_match = self.repository.update_match(match)
        logger.info(f"Match updated: {updated_match}")
        return updated_match

    # ------------------- DELETE -------------------
    def delete_match(self, source: str, target: str) -> bool:
        result = self.repository.delete_match(source, target)
        logger.info(f"Match deleted: source={source}, target={target}, success={result}")
        return result
