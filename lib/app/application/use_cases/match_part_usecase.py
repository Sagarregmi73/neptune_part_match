class MatchPartUseCase:
    def __init__(self, repository):
        self.repository = repository

    async def create_match(self, match):
        return await self.repository.create_match(match)

    async def get_match(self, source, target):
        return await self.repository.get_match(source, target)

    async def update_match(self, match):
        return await self.repository.update_match(match)

    async def delete_match(self, source, target):
        return await self.repository.delete_match(source, target)

    async def list_matches(self):
        return await self.repository.list_matches()

    async def get_matches_for_part(self, part_number):
        return await self.repository.get_matches_for_part(part_number)
