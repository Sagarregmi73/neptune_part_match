class PartNumber:
    """
    Represents a PartNumber with dynamic specs and notes as dictionaries
    """
    def __init__(self, part_number: str, specs: dict, notes: dict = None):
        self.part_number = part_number
        self.specs = specs or {}
        self.notes = notes or {}
