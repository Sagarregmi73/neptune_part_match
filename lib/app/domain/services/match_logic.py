# lib/app/domain/services/match_logic.py

class MatchLogic:
    """
    Determines the match type between two parts based on specs and notes.
    """

    @staticmethod
    def determine_match(
        input_specs: dict,
        input_notes: dict,
        output_specs: dict,
        output_notes: dict
    ) -> str:
        """
        Returns match type: "Perfect", "Partial", or "No Match"
        """

        # Perfect match: all specs AND notes match exactly
        if input_specs == output_specs and input_notes == output_notes:
            return "Perfect"

        # Partial match: at least one spec or one note matches
        spec_match = any(
            input_specs.get(f"spec{i}") == output_specs.get(f"spec{i}")
            for i in range(1, 6)
        )
        note_match = any(
            input_notes.get(f"note{i}") == output_notes.get(f"note{i}")
            for i in range(1, 4)
        )

        if spec_match or note_match:
            return "Partial"

        # No match: nothing matches
        return "No Match"
