# lib/app/application/use_cases/crud_part_usecase.py

from typing import List, Optional
from lib.app.domain.entities.part_number import PartNumber

class CrudPartUseCase:
    """
    Handles CRUD operations for PartNumber entities.
    """

    def __init__(self, repository):
        self.repository = repository

    # ---------- CREATE ----------
    def create_part(self, part: PartNumber) -> PartNumber:
        """
        Create a new part in the repository.
        """
        created_part = self.repository.create_part(part)
        # After creation, attach the unique ID from the repository if available
        if hasattr(part, "id"):
            created_part.id = part.id
        return created_part

    # ---------- READ ----------
    def get_part(self, part_number: str) -> Optional[PartNumber]:
        """
        Get a part by its part_number.
        """
        return self.repository.get_part(part_number)

    # ---------- UPDATE ----------
    def update_part(self, part: PartNumber) -> PartNumber:
        """
        Update the specs/notes of a part.
        """
        updated_part = self.repository.update_part(part)
        # Ensure the unique ID remains attached
        if hasattr(part, "id"):
            updated_part.id = part.id
        return updated_part

    # ---------- DELETE ----------
    def delete_part(self, part_number: str) -> bool:
        """
        Delete a part by its part_number.
        """
        return self.repository.delete_part(part_number)

    # ---------- LIST ----------
    def list_parts(self) -> List[PartNumber]:
        """
        List all parts.
        """
        return self.repository.list_parts()
