import pandas as pd
import tempfile
import os
from io import BytesIO
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.aws.s3_client import upload_file_to_s3

class UploadFileUseCase:
    def execute(self, file_bytes: BytesIO, filename: str):
        # 1️⃣ Save XLSX temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes.read())
            tmp_path = tmp.name

        # 2️⃣ Convert Excel to CSV for Neptune bulk load
        df = pd.read_excel(tmp_path)
        vertices, edges = [], []

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

        # 3️⃣ Save CSV temporarily
        vertices_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        edges_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        vertices_df.to_csv(vertices_csv, index=False)
        edges_df.to_csv(edges_csv, index=False)

        # 4️⃣ Upload CSVs to S3
        vertices_s3_key = f"neptune_bulk/{os.path.basename(vertices_csv)}"
        edges_s3_key = f"neptune_bulk/{os.path.basename(edges_csv)}"
        upload_file_to_s3(vertices_csv, vertices_s3_key)
        upload_file_to_s3(edges_csv, edges_s3_key)

        # 5️⃣ Trigger Neptune Bulk Loader
        s3_folder_uri = f"s3://{os.getenv('S3_BUCKET_NAME')}/neptune_bulk/"
        bulk_response = trigger_bulk_load(s3_folder_uri)

        return {
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "s3_vertices": vertices_s3_key,
            "s3_edges": edges_s3_key,
            "bulk_load_id": bulk_response.get("loadId")
        }
