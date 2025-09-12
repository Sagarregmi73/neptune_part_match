# lib/app/application/services/repository_interface.py
from abc import ABC, abstractmethod
from typing import List, Optional
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class RepositoryInterface(ABC):
    """
    Abstract repository interface for CRUD operations on PartNumber and Match.
    Any concrete repository (Neptune/Postgres/Mongo) must implement these methods.
    """

    # ------------------- PART NUMBER CRUD -------------------
    @abstractmethod
    def create_part(self, part: PartNumber) -> PartNumber:
        """
        Create or upsert a PartNumber vertex in the database.
        """
        pass

    @abstractmethod
    def get_part(self, part_number: str) -> Optional[PartNumber]:
        """
        Retrieve a PartNumber by its unique ID.
        """
        pass

    @abstractmethod
    def update_part(self, part: PartNumber) -> PartNumber:
        """
        Update an existing PartNumber's properties.
        """
        pass

    @abstractmethod
    def delete_part(self, part_number: str) -> bool:
        """
        Delete a PartNumber from the database.
        """
        pass

    @abstractmethod
    def list_parts(self) -> List[PartNumber]:
        """
        List all PartNumbers in the database.
        """
        pass

    # ------------------- MATCH CRUD -------------------
    @abstractmethod
    def create_match(self, match: Match) -> Match:
        """
        Create a Match edge (replacement) between PartNumbers.
        """
        pass

    @abstractmethod
    def get_match(self, source: str, target: str) -> Optional[Match]:
        """
        Retrieve a Match between two PartNumbers.
        """
        pass

    @abstractmethod
    def update_match(self, match: Match) -> Match:
        """
        Update the match_type of an existing Match edge.
        """
        pass

    @abstractmethod
    def delete_match(self, source: str, target: str) -> bool:
        """
        Delete a Match edge between two PartNumbers.
        """
        pass

    @abstractmethod
    def list_matches(self) -> List[Match]:
        """
        List all Match edges in the database.
        """
        pass
