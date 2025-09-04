# lib/app/domain/entities/match.py

class Match:
    def __init__(self, source: str, target: str, match_type: str):
        """
        source: Input Part Number
        target: Output Part Number
        match_type: Perfect / Partial / No Match
        """
        self.source = source
        self.target = target
        self.match_type = match_type

    def __repr__(self):
        return f"Match(source={self.source}, target={self.target}, match_type={self.match_type})"
