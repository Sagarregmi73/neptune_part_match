# lib/app/application/use_cases/upload_file_usecase.py

import pandas as pd
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.domain.services.match_logic import MatchLogic
import boto3
from botocore.exceptions import NoCredentialsError
from io import BytesIO
import os


class UploadFileUseCase:
    def __init__(self, backup_to_s3: bool = False):
        self.repo = NeptuneRepository()
        self.logic = MatchLogic()
        self.backup_to_s3 = backup_to_s3

    def _upload_to_s3(self, file_bytes, filename: str):
        """
        Upload file to S3 using credentials from environment variables.
        """
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION")
            )
            bucket_name = os.getenv("S3_BUCKET_NAME")
            if not bucket_name:
                raise ValueError("S3_BUCKET_NAME not set in environment")
            s3_client.upload_fileobj(BytesIO(file_bytes), bucket_name, filename)
            return True
        except NoCredentialsError:
            print("S3 credentials not found!")
            return False
        except Exception as e:
            print(f"S3 upload failed: {e}")
            return False

    def execute(self, file_bytes, filename: str):
        """
        Upload Excel file, create parts and matches in Neptune.
        """
        # Backup to S3 if enabled
        if self.backup_to_s3:
            if isinstance(file_bytes, BytesIO):
                raw_bytes = file_bytes.getvalue()
            else:
                raw_bytes = file_bytes
            self._upload_to_s3(raw_bytes, filename)

        # Read Excel with multi-level headers
        df = pd.read_excel(file_bytes, header=[0, 1])

        # Flatten MultiIndex columns: ('Input Spec', 'Length') -> 'Input Spec_Length'
        df.columns = ['_'.join([str(c) for c in col if c]) for col in df.columns]

        vertices_created = set()
        edges_created = 0

        # Iterate rows using iterrows (safe for columns with spaces)
        for i, row in df.iloc[2:].iterrows():  # skip first 2 template rows
            # Extract part numbers
            input_part = row.get("Input Part Number", None)
            output_part = row.get("Output Part Number", None)

            # Extract specs and notes dynamically
            input_specs = {col: row[col] for col in df.columns if col.startswith("Input Spec")}
            input_notes = {col: row[col] for col in df.columns if col.startswith("Input Note")}
            output_specs = {col: row[col] for col in df.columns if col.startswith("Output Spec")}
            output_notes = {col: row[col] for col in df.columns if col.startswith("Output Note")}

            # Create vertices for parts
            if input_part and input_part not in vertices_created:
                self.repo.create_part(PartNumber(input_part, input_specs, input_notes))
                vertices_created.add(input_part)

            if output_part and output_part not in vertices_created:
                self.repo.create_part(PartNumber(output_part, output_specs, output_notes))
                vertices_created.add(output_part)

            # Determine match type dynamically and create edge
            if input_part and output_part:
                match_type = self.logic.determine_match(input_specs, input_notes, output_specs, output_notes)
                self.repo.create_match(Match(input_part, output_part, match_type))
                edges_created += 1

        return {"vertices_created": len(vertices_created), "edges_created": edges_created}
