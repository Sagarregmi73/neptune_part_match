# Example for MatchPartUseCase
class MatchPartUseCase:
    def __init__(self, repository):
        self.repository = repository

    def create_match(self, match):
        return self.repository.create_match(match)

    def get_match(self, source, target):
        return self.repository.get_match(source, target)

    def update_match(self, match):
        return self.repository.update_match(match)

    def delete_match(self, source, target):
        return self.repository.delete_match(source, target)

    def list_matches(self):
        return self.repository.list_matches()

    def get_matches_for_part(self, part_number):
        return self.repository.get_matches_for_part(part_number)
