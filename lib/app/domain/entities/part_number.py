# lib/app/domain/entities/part_number.py

class PartNumber:
    def __init__(self, part_number: str, specs: str, notes: str = ""):
        self.part_number = part_number
        self.specs = specs
        self.notes = notes

    def __repr__(self):
        return f"PartNumber(part_number={self.part_number}, specs={self.specs}, notes={self.notes})"
