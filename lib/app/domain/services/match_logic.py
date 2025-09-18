class MatchLogic:
    """
    Determine match type between two parts
    """

    @staticmethod
    def determine_match(input_specs: dict, input_notes: dict, output_specs: dict, output_notes: dict) -> str:
        # Perfect: all specs and notes match
        if input_specs == output_specs and input_notes == output_notes:
            return "Perfect"
        # Partial: first key of specs matches
        elif list(input_specs.values())[0] == list(output_specs.values())[0]:
            return "Partial"
        else:
            return "No Match"
