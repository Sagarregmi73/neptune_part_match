import pandas as pd
import tempfile
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.core.aws.s3_client import upload_file_to_s3
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load

class UploadFileUseCase:
    def __init__(self, backup_to_s3: bool = True):
        self.repo = NeptuneRepository()
        self.backup_to_s3 = backup_to_s3

    def execute(self, file_bytes, filename: str):
        df = pd.read_excel(file_bytes)
        vertices_created = set()
        edges_created = 0

        for row in df.itertuples():
            input_part = getattr(row, "Input_Part_Number", None)
            output_part = getattr(row, "Output_Part_Number", None)
            input_specs = getattr(row, "Input_Specs", "")
            output_specs = getattr(row, "Output_Specs", "")
            input_notes = getattr(row, "Input_Notes", "")
            output_notes = getattr(row, "Output_Notes", "")
            match_type_raw = getattr(row, "Match_Type", None)

            if input_part and input_part not in vertices_created:
                self.repo.create_part(PartNumber(input_part, input_specs, input_notes))
                vertices_created.add(input_part)

            if output_part and output_part != "-" and output_part not in vertices_created:
                self.repo.create_part(PartNumber(output_part, output_specs, output_notes))
                vertices_created.add(output_part)

            if output_part and output_part != "-" and match_type_raw:
                match_type = "Replacement" if match_type_raw in ["Perfect", "Partial"] else "No Replacement"
                self.repo.create_match(Match(input_part, output_part, match_type))
                edges_created += 1

        # Optional CSV backup
        if self.backup_to_s3:
            vertices_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            edges_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

            vertices_data = []
            for row in df.itertuples():
                if getattr(row, "Input_Part_Number", None):
                    vertices_data.append({"part_number": row.Input_Part_Number,
                                          "specs": row.Input_Specs,
                                          "notes": row.Input_Notes})
                if getattr(row, "Output_Part_Number", None) and row.Output_Part_Number != "-":
                    vertices_data.append({"part_number": row.Output_Part_Number,
                                          "specs": row.Output_Specs,
                                          "notes": row.Output_Notes})
            pd.DataFrame(vertices_data).drop_duplicates(subset="part_number")\
                .to_csv(vertices_file.name, index=False)

            edges_file.write(b"source,target,match_type\n")
            for row in df.itertuples():
                if getattr(row, "Match_Type", None):
                    match_type = "Replacement" if row.Match_Type in ["Perfect", "Partial"] else "No Replacement"
                    edges_file.write(f"{row.Input_Part_Number},{row.Output_Part_Number},{match_type}\n".encode())
            edges_file.close()

            vertices_s3 = upload_file_to_s3(vertices_file.name, f"vertices/{filename}.csv")
            edges_s3 = upload_file_to_s3(edges_file.name, f"edges/{filename}.csv")
            trigger_bulk_load(vertices_s3, edges_s3)

            return {"status": "success", "vertices_s3": vertices_s3, "edges_s3": edges_s3}

        return {"status": "success", "vertices_created": len(vertices_created), "edges_created": edges_created}
