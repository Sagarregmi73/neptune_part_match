import pandas as pd
import tempfile
from lib.core.aws.s3_client import upload_file_to_s3
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class UploadFileUseCase:
    """
    Handles Excel uploads and inserts vertices and edges into Neptune,
    including Replacement / No Replacement logic and CSV backup.
    """

    def __init__(self, backup_to_s3: bool = True):
        self.repo = NeptuneRepository()
        self.backup_to_s3 = backup_to_s3

    def execute(self, file_bytes: bytes, filename: str):
        df = pd.read_excel(file_bytes)

        # ----------------- Insert vertices and edges into Neptune -----------------
        for row in df.itertuples():
            # Create Input Part vertex
            if row.InputPartNumber and not self.repo.get_part(row.InputPartNumber):
                self.repo.create_part(PartNumber(
                    part_number=row.InputPartNumber,
                    specs=row.InputSpecs,
                    notes=row.InputNotes
                ))

            # Create Output Part vertex
            if row.OutputPartNumber and not self.repo.get_part(row.OutputPartNumber):
                self.repo.create_part(PartNumber(
                    part_number=row.OutputPartNumber,
                    specs=row.OutputSpecs,
                    notes=row.OutputNotes
                ))

            # Determine match_type for edge
            if getattr(row, 'MatchType', None):
                match_type = "Replacement" if row.MatchType in ["Perfect", "Partial"] else "No Replacement"

                # Create edge in Neptune
                self.repo.create_match(Match(
                    source=row.InputPartNumber,
                    target=row.OutputPartNumber,
                    match_type=match_type
                ))

        # ----------------- Optional CSV backup -----------------
        if self.backup_to_s3:
            vertices_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            edges_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

            # Vertices CSV
            vertices_data = []
            for row in df.itertuples():
                if row.InputPartNumber:
                    vertices_data.append({
                        "part_number": row.InputPartNumber,
                        "specs": row.InputSpecs,
                        "notes": row.InputNotes
                    })
                if row.OutputPartNumber:
                    vertices_data.append({
                        "part_number": row.OutputPartNumber,
                        "specs": row.OutputSpecs,
                        "notes": row.OutputNotes
                    })
            df_vertices = pd.DataFrame(vertices_data).drop_duplicates(subset="part_number")
            df_vertices.to_csv(vertices_file.name, index=False)

            # Edges CSV
            edges_file.write(b"source,target,match_type\n")
            for row in df.itertuples():
                if getattr(row, 'MatchType', None):
                    match_type = "Replacement" if row.MatchType in ["Perfect", "Partial"] else "No Replacement"
                    line = f"{row.InputPartNumber},{row.OutputPartNumber},{match_type}\n"
                    edges_file.write(line.encode())
            edges_file.close()

            # Upload CSVs to S3
            vertices_s3 = upload_file_to_s3(vertices_file.name, f"vertices/{filename}.csv")
            edges_s3 = upload_file_to_s3(edges_file.name, f"edges/{filename}.csv")

            # Trigger Neptune bulk load (optional)
            trigger_bulk_load(vertices_s3, edges_s3)

            return {
                "status": "success",
                "message": "Excel loaded into Neptune and CSV backup created",
                "vertices_s3": vertices_s3,
                "edges_s3": edges_s3
            }

        return {"status": "success", "message": "Excel loaded into Neptune dynamically"}
