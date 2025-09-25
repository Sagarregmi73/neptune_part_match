from lib.app.application.services.repository_interface import RepositoryInterface
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class CrudPartUseCase:
    def __init__(self, repository: RepositoryInterface):
        self.repository = repository

    async def create_part(self, part: PartNumber) -> PartNumber:
        return await self.repository.create_part(part)

    async def get_part(self, part_number: str):
        return await self.repository.get_part(part_number)

    async def update_part(self, part: PartNumber) -> PartNumber:
        return await self.repository.update_part(part)

    async def delete_part(self, part_number: str) -> bool:
        return await self.repository.delete_part(part_number)

    async def list_parts(self):
        return await self.repository.list_parts()

    # ---------------- Matches ----------------
    async def create_match(self, match: Match) -> Match:
        return await self.repository.create_match(match)

    async def get_match(self, source: str, target: str):
        return await self.repository.get_match(source, target)

    async def update_match(self, match: Match) -> Match:
        return await self.repository.update_match(match)

    async def delete_match(self, source: str, target: str) -> bool:
        return await self.repository.delete_match(source, target)

    async def list_matches(self):
        return await self.repository.list_matches()

    async def get_matches_for_part(self, part_number: str):
        return await self.repository.get_matches_for_part(part_number)
