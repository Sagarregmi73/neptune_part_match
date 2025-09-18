from abc import ABC, abstractmethod
from typing import List, Optional
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class RepositoryInterface(ABC):
    """
    Abstract interface for repository operations.
    This ensures all repository implementations (Neptune, etc.) follow the same contract.
    """

    # -------------------- PART CRUD --------------------
    @abstractmethod
    def create_part(self, part: PartNumber) -> PartNumber:
        pass

    @abstractmethod
    def get_part(self, part_number: str) -> Optional[PartNumber]:
        pass

    @abstractmethod
    def update_part(self, part: PartNumber) -> PartNumber:
        pass

    @abstractmethod
    def delete_part(self, part_number: str) -> bool:
        pass

    @abstractmethod
    def list_parts(self) -> List[PartNumber]:
        pass

    # -------------------- MATCH CRUD --------------------
    @abstractmethod
    def create_match(self, match: Match) -> Match:
        pass

    @abstractmethod
    def get_match(self, source: str, target: str) -> Optional[Match]:
        pass

    @abstractmethod
    def update_match(self, match: Match) -> Match:
        pass

    @abstractmethod
    def delete_match(self, source: str, target: str) -> bool:
        pass

    @abstractmethod
    def list_matches(self) -> List[Match]:
        pass

    # -------------------- BIDIRECTIONAL SEARCH --------------------
    @abstractmethod
    def get_matches_for_part(self, part_number: str) -> List[Match]:
        """
        Returns all matches where the part is either source or target.
        """
        pass
