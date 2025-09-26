# lib/app/domain/entities/part_number.py

class PartNumber:
    def __init__(self, part_number: str, spec1: str, spec2: str, spec3: str, spec4: str, spec5: str,
                 note1: str = "", note2: str = "", note3: str = "", id: str = None):
        self.id = id
        self.part_number = part_number
        self.spec1 = spec1
        self.spec2 = spec2
        self.spec3 = spec3
        self.spec4 = spec4
        self.spec5 = spec5
        self.note1 = note1
        self.note2 = note2
        self.note3 = note3
