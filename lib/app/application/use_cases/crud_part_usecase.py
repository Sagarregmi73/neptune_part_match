class CrudPartUseCase:
    def __init__(self, repository):
        self.repository = repository

    def create_part(self, part):
        return self.repository.create_part(part)

    def get_part(self, part_number):
        return self.repository.get_part(part_number)

    def update_part(self, part):
        return self.repository.update_part(part)

    def delete_part(self, part_number):
        return self.repository.delete_part(part_number)

    def list_parts(self):
        return self.repository.list_parts()
