class Match:
    """
    Represents a match (replacement) between two parts.
    """
    def __init__(self, source: str, target: str, match_type: str):
        self.source = source
        self.target = target
        self.match_type = match_type
