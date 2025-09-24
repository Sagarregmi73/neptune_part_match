from typing import List, Optional
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.match import Match
from lib.app.domain.services.match_logic import MatchLogic
from lib.core.logging import logger

class MatchService:
    """
    Service layer for handling Match CRUD operations and business logic.
    """

    def __init__(self, repository: RepositoryInterface):
        self.repository = repository
        self.logic = MatchLogic()

    # ------------------- CREATE -------------------
    def create_match(
        self,
        source: str,
        target: str,
        source_specs: Optional[dict] = None,
        source_notes: Optional[dict] = None,
        target_specs: Optional[dict] = None,
        target_notes: Optional[dict] = None,
        match_type: Optional[str] = None
    ) -> Match:
        """
        Create a match. If match_type not provided, determine automatically.
        """
        if not match_type and all([source_specs, source_notes, target_specs, target_notes]):
            match_type = self.logic.determine_match(source_specs, source_notes, target_specs, target_notes)

        match = Match(source=source, target=target, match_type=match_type or "No Match")
        self.repository.create_match(match)
        logger.info(f"Match created: {match.source} -> {match.target} ({match.match_type})")
        return match

    # ------------------- READ -------------------
    def get_match(self, source: str, target: str) -> Optional[Match]:
        return self.repository.get_match(source, target)

    def list_matches(self) -> List[Match]:
        return self.repository.list_matches()

    # ------------------- UPDATE -------------------
    def update_match(self, match: Match) -> Match:
        updated_match = self.repository.update_match(match)
        logger.info(f"Match updated: {updated_match.source} -> {updated_match.target} ({updated_match.match_type})")
        return updated_match

    # ------------------- DELETE -------------------
    def delete_match(self, source: str, target: str) -> bool:
        result = self.repository.delete_match(source, target)
        logger.info(f"Match deleted: {source} -> {target}, success={result}")
        return result

    # ------------------- BIDIRECTIONAL SEARCH -------------------
    def search_matches_for_part(self, part_number: str) -> List[Match]:
        """
        Returns all matches where part_number is either source or target.
        """
        return self.repository.get_matches_for_part(part_number)
