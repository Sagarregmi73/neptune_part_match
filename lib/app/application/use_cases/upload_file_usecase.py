# lib/app/application/use_cases/upload_file_usecase.py

import pandas as pd
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.domain.services.match_logic import MatchLogic
import boto3
from botocore.exceptions import NoCredentialsError
import io

class UploadFileUseCase:
    def __init__(self, backup_to_s3: bool = False):
        self.repo = NeptuneRepository()
        self.logic = MatchLogic()
        self.backup_to_s3 = backup_to_s3

    def _upload_to_s3(self, file_bytes, filename: str):
        try:
            s3_client = boto3.client("s3")
            bucket_name = "your-bucket-name"  # Update with your bucket
            s3_client.upload_fileobj(io.BytesIO(file_bytes), bucket_name, filename)
            return True
        except NoCredentialsError:
            print("S3 credentials not found!")
            return False
        except Exception as e:
            print(f"S3 upload failed: {e}")
            return False

    def execute(self, file_bytes, filename: str):
        # Backup to S3 if enabled
        if self.backup_to_s3:
            self._upload_to_s3(file_bytes, filename)

        # Read Excel
        df = pd.read_excel(file_bytes, header=[0, 1])  # first 2 rows: header + template
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
