# lib/app/application/use_cases/match_part_usecase.py

from typing import List, Optional
from lib.app.domain.entities.match import Match

class MatchPartUseCase:
    """
    Handles CRUD operations for Match entities.
    """

    def __init__(self, repository):
        self.repository = repository

    # ---------- CREATE ----------
    def create_match(self, match: Match) -> Match:
        """
        Create a match between two parts.
        """
        created_match = self.repository.create_match(match)
        return created_match

    # ---------- READ ----------
    def get_match(self, source: str, target: str) -> Optional[Match]:
        """
        Get a match by source and target part numbers.
        """
        return self.repository.get_match(source, target)

    # ---------- UPDATE ----------
    def update_match(self, match: Match) -> Match:
        """
        Update the match_type for a match.
        """
        updated_match = self.repository.update_match(match)
        return updated_match

    # ---------- DELETE ----------
    def delete_match(self, source: str, target: str) -> bool:
        """
        Delete a match by source and target part numbers.
        """
        return self.repository.delete_match(source, target)

    # ---------- LIST ----------
    def list_matches(self) -> List[Match]:
        """
        List all matches.
        """
        return self.repository.list_matches()

    # ---------- GET MATCHES FOR PART ----------
    def get_matches_for_part(self, part_number: str):
        """
        Returns matches for a specific part in the format:
        {
            "part": PartNumber,
            "matches": [
                {
                    "replacement_part": PartNumber,
                    "match_type": str
                }, ...
            ]
        }
        """
        return self.repository.get_matches_for_part(part_number)
