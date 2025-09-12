class MatchLogic:
    @staticmethod
    def determine_match(input_specs: str, input_notes: str, output_specs: str, output_notes: str) -> str:
        if input_specs == output_specs and input_notes == output_notes:
            return "Perfect"
        elif input_specs.split(",")[0] == output_specs.split(",")[0]:
            return "Partial"
        else:
            return "No Match"
