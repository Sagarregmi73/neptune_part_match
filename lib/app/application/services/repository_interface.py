# lib/app/application/services/repository_interface.py
from abc import ABC, abstractmethod
from typing import List, Optional
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class RepositoryInterface(ABC):
    """
    Abstract interface for CRUD operations on PartNumbers and Matches.
    Concrete repositories (Neptune/Postgres/Mongo) must implement these methods.
    """

    # ----- PART NUMBER CRUD -----
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

    @abstractmethod
    def list_matches(self) -> List[Match]:
        pass

    # ----- MATCH CRUD -----
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
