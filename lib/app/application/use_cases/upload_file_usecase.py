# lib/app/application/use_cases/upload_file_usecase.py

import pandas as pd
import tempfile
import os
from io import BytesIO
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.aws.s3_client import get_s3_client

class UploadFileUseCase:
    def __init__(self, backup_to_s3: bool = True):
        self.backup_to_s3 = backup_to_s3
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.s3_client = get_s3_client()

    def execute(self, file_bytes, filename: str):
        # Step 1: Save XLSX temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        # Step 2: Read Excel
        df = pd.read_excel(tmp_path)

        vertices = []
        edges = []

        for _, row in df.iterrows():
            input_part = row["Input Part Number"]
            output_part = row["Output Part Number"]

            # Input vertex
            input_props = {k: row[k] for k in df.columns if k.startswith("Input Spec") or k.startswith("Input Note")}
            vertices.append({"~id": input_part, "~label": "PartNumber", **input_props})

            # Output vertex
            output_props = {k: row[k] for k in df.columns if k.startswith("Output Spec") or k.startswith("Output Note")}
            vertices.append({"~id": output_part, "~label": "PartNumber", **output_props})

            # Edge
            match_type = row.get("Match Type", "No Match")
            edges.append({"~from": input_part, "~to": output_part, "~label": "MATCHED", "match_type": match_type})

        # Remove duplicate vertices
        vertices_df = pd.DataFrame(vertices).drop_duplicates(subset="~id")
        edges_df = pd.DataFrame(edges)

        # Step 3: Save CSVs temporarily
        vertices_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        edges_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        vertices_df.to_csv(vertices_csv, index=False)
        edges_df.to_csv(edges_csv, index=False)

        # Step 4: Upload CSVs to S3
        vertices_s3_key = f"neptune_bulk/{os.path.basename(vertices_csv)}"
        edges_s3_key = f"neptune_bulk/{os.path.basename(edges_csv)}"

        if self.backup_to_s3:
            self._upload_to_s3(vertices_csv, vertices_s3_key)
            self._upload_to_s3(edges_csv, edges_s3_key)

        # Step 5: Trigger Neptune Bulk Loader
        s3_folder_uri = f"s3://{self.s3_bucket}/neptune_bulk/"
        bulk_response = trigger_bulk_load(s3_folder_uri, mode="NEW")

        return {
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "s3_vertices": vertices_s3_key,
            "s3_edges": edges_s3_key,
            "bulk_load_id": bulk_response.get("loadId")
        }

    def _upload_to_s3(self, local_file_path, s3_key):
        try:
            self.s3_client.upload_file(local_file_path, self.s3_bucket, s3_key)
            print(f"Uploaded {s3_key} to bucket {self.s3_bucket}")
        except Exception as e:
            raise Exception(f"S3 upload failed: {e}")
