# lib/app/application/use_cases/upload_file_usecase.py
import pandas as pd
import tempfile
import os
from io import BytesIO
from lib.core.aws.s3_client import upload_file_to_s3
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.app.domain.services.match_logic import MatchLogic
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase


class UploadFileUseCase:
    def __init__(self, part_usecase: CrudPartUseCase, match_usecase: MatchPartUseCase, backup_to_s3: bool = True):
        self.backup_to_s3 = backup_to_s3
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.part_usecase = part_usecase
        self.match_usecase = match_usecase
        self.logic = MatchLogic()

    def execute(self, file_bytes: BytesIO, filename: str) -> dict:
        # Step 1: Save XLSX temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes.read())
            tmp_path = tmp.name

        # Step 2: Read Excel
        df = pd.read_excel(tmp_path)

        vertices, edges = [], []

        for _, row in df.iterrows():
            # --- Input Part ---
            input_part_number = row["Input Part Number"]
            input_specs = {f"spec{i}": row[f"Input Spec {i}"] for i in range(1, 6)}
            input_notes = {f"note{i}": row[f"Input Note {i}"] for i in range(1, 4)}

            input_part = PartNumber(input_part_number, input_specs, input_notes)
            self.part_usecase.create_part(input_part)

            # --- Output Part ---
            output_part_number = row["Output Part Number"]
            output_specs = {f"spec{i}": row[f"Output Spec {i}"] for i in range(1, 6)}
            output_notes = {f"note{i}": row[f"Output Note {i}"] for i in range(1, 3+1)}

            output_part = PartNumber(output_part_number, output_specs, output_notes)
            self.part_usecase.create_part(output_part)

            # --- Determine Match Type ---
            match_type = row.get("Match Type")
            if not match_type or str(match_type).lower() in ["", "auto"]:
                match_type = self.logic.determine_match(input_specs, input_notes, output_specs, output_notes)

            # --- Save Match ---
            match = Match(input_part_number, output_part_number, match_type)
            self.match_usecase.create_match(match)

            # Keep for CSV / bulk loader
            vertices.append({"~id": input_part_number, "~label": "PartNumber", **input_specs, **input_notes})
            vertices.append({"~id": output_part_number, "~label": "PartNumber", **output_specs, **output_notes})
            edges.append({"~from": input_part_number, "~to": output_part_number, "~label": "MATCHED", "match_type": match_type})

        # --- Optional: Upload CSVs for Neptune Bulk Loader ---
        vertices_df = pd.DataFrame(vertices).drop_duplicates(subset="~id")
        edges_df = pd.DataFrame(edges)

        vertices_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        edges_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        vertices_df.to_csv(vertices_csv, index=False)
        edges_df.to_csv(edges_csv, index=False)

        vertices_s3_key = f"neptune_bulk/{os.path.basename(vertices_csv)}"
        edges_s3_key = f"neptune_bulk/{os.path.basename(edges_csv)}"

        if self.backup_to_s3:
            upload_file_to_s3(vertices_csv, vertices_s3_key)
            upload_file_to_s3(edges_csv, edges_s3_key)

        s3_folder_uri = f"s3://{self.s3_bucket}/neptune_bulk/"
        bulk_response = trigger_bulk_load(s3_folder_uri, mode="NEW")

        bulk_load_id = None
        if isinstance(bulk_response, dict):
            bulk_load_id = bulk_response.get("payload", {}).get("loadId") or bulk_response.get("loadId")

        return {
            "message": "File processed and matches created successfully",
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "s3_vertices": vertices_s3_key,
            "s3_edges": edges_s3_key,
            "bulk_load_id": bulk_load_id
        }
