import pandas as pd
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.domain.services.match_logic import MatchLogic

class UploadFileUseCase:
    def __init__(self):
        self.repo = NeptuneRepository()
        self.logic = MatchLogic()

    def execute(self, file_bytes, filename: str):
        df = pd.read_excel(file_bytes, header=[0,1])  # first 2 rows: header + template
        vertices_created = set()
        edges_created = 0

        for row in df.iloc[2:].itertuples(index=False):
            input_part = getattr(row, "Input Part Number", None)
            output_part = getattr(row, "Output Part Number", None)

            input_specs = {col[1]: row[col] for col in df.columns if col[0].startswith("Input Spec")}
            input_notes = {col[1]: row[col] for col in df.columns if col[0].startswith("Input Note")}
            output_specs = {col[1]: row[col] for col in df.columns if col[0].startswith("Output Spec")}
            output_notes = {col[1]: row[col] for col in df.columns if col[0].startswith("Output Note")}

            # Create vertices
            if input_part and input_part not in vertices_created:
                self.repo.create_part(PartNumber(input_part, input_specs, input_notes))
                vertices_created.add(input_part)

            if output_part and output_part not in vertices_created:
                self.repo.create_part(PartNumber(output_part, output_specs, output_notes))
                vertices_created.add(output_part)

            # Determine match type dynamically
            if input_part and output_part:
                match_type = self.logic.determine_match(input_specs, input_notes, output_specs, output_notes)
                self.repo.create_match(Match(input_part, output_part, match_type))
                edges_created += 1

        return {"vertices_created": len(vertices_created), "edges_created": edges_created}
