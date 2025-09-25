class CrudPartUseCase:
    def __init__(self, repository):
        self.repository = repository

    async def create_part(self, part):
        return await self.repository.create_part(part)

    async def get_part(self, part_number):
        return await self.repository.get_part(part_number)

    async def update_part(self, part):
        return await self.repository.update_part(part)

    async def delete_part(self, part_number):
        return await self.repository.delete_part(part_number)

    async def list_parts(self):
        return await self.repository.list_parts()
