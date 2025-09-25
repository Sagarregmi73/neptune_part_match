from lib.app.domain.entities.match import Match
from lib.app.domain.dtos.match_dto import MatchDTO
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository


class MatchPartUseCase:
    def __init__(self, repository: NeptuneRepository):
        self.repository = repository

    def create_match(self, match: Match) -> MatchDTO:
        saved = self.repository.save_match(match)
        return MatchDTO(**saved.__dict__)

    def get_match(self, source: str, target: str) -> MatchDTO | None:
        match = self.repository.get_match(source, target)
        return MatchDTO(**match.__dict__) if match else None

    def update_match(self, match: Match) -> MatchDTO:
        updated = self.repository.update_match(match)
        return MatchDTO(**updated.__dict__)

    def delete_match(self, source: str, target: str) -> bool:
        return self.repository.delete_match(source, target)

    def list_matches(self) -> list[MatchDTO]:
        matches = self.repository.list_matches()
        return [MatchDTO(**m.__dict__) for m in matches]

    def get_matches_for_part(self, part_number: str) -> dict:
        # Get the main search part
        main_part = self.repository.get_part(part_number)
        if not main_part:
            return {"error": f"Part {part_number} not found"}

        # Get all matches (both directions)
        matches = self.repository.get_matches_for_part(part_number)

        # Prepare replacement list
        replacements = []
        for m in matches:
            if m.match_type.lower() in ["perfect", "partial"]:
                # Get full info for replacement part
                replacement_part = self.repository.get_part(m.in_part(part_number))
                if replacement_part:
                    replacements.append({
                        "part_number": replacement_part.part_number,
                        "spec1": replacement_part.spec1,
                        "spec2": replacement_part.spec2,
                        "spec3": replacement_part.spec3,
                        "spec4": replacement_part.spec4,
                        "spec5": replacement_part.spec5,
                        "note1": replacement_part.note1,
                        "note2": replacement_part.note2,
                        "note3": replacement_part.note3,
                        "match_type": m.match_type
                    })

        return {
            "search_part": {
                "part_number": main_part.part_number,
                "spec1": main_part.spec1,
                "spec2": main_part.spec2,
                "spec3": main_part.spec3,
                "spec4": main_part.spec4,
                "spec5": main_part.spec5,
                "note1": main_part.note1,
                "note2": main_part.note2,
                "note3": main_part.note3
            },
            "replacements": replacements
        }
