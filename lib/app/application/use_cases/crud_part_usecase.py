# lib/app/application/use_cases/crud_part_usecase.py
from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber

class CrudPartUseCase:
    def __init__(self, repository: RepositoryInterface):
        self.repository = repository

    def create_part(self, part: PartNumber) -> PartNumber:
        return self.repository.create_part(part)

    def get_part(self, part_number: str):
        return self.repository.get_part(part_number)

    def update_part(self, part: PartNumber) -> PartNumber:
        return self.repository.update_part(part)

    def delete_part(self, part_number: str) -> bool:
        return self.repository.delete_part(part_number)

    def list_parts(self):
        return self.repository.list_parts()
