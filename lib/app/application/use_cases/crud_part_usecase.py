from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.dtos.part_number_dto import PartNumberDTO
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository


class CrudPartUseCase:
    def __init__(self, repository: NeptuneRepository):
        self.repository = repository

    def create_part(self, part: PartNumber) -> PartNumberDTO:
        saved = self.repository.create_part(part)  # <-- fixed here
        return PartNumberDTO(**saved.__dict__)

    def get_part(self, part_number: str) -> PartNumberDTO | None:
        part = self.repository.get_part(part_number)
        return PartNumberDTO(**part.__dict__) if part else None

    def update_part(self, part: PartNumber) -> PartNumberDTO:
        updated = self.repository.update_part(part)
        return PartNumberDTO(**updated.__dict__)

    def delete_part(self, part_number: str) -> bool:
        return self.repository.delete_part(part_number)

    def list_parts(self) -> list[PartNumberDTO]:
        parts = self.repository.list_parts()
        return [PartNumberDTO(**p.__dict__) for p in parts]
