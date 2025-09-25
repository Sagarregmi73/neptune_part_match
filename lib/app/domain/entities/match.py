class Match:
    def __init__(self, source: str, target: str, match_type: str):
        self.source = source
        self.target = target
        self.match_type = match_type

    def in_part(self, part_number: str):
        """Return the other part in the match given one part_number"""
        return self.target if self.source == part_number else self.source
