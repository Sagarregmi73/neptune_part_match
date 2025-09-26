# lib/app/application/use_cases/upload_file_usecase.py

import pandas as pd
import tempfile
from io import BytesIO
import asyncio
import os
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.domain.services.match_logic import MatchLogic
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.aws.s3_client import upload_file_to_s3_async

class UploadFileUseCase:
    def __init__(self, part_usecase, match_usecase, backup_to_s3=True):
        self.part_usecase = part_usecase
        self.match_usecase = match_usecase
        self.backup_to_s3 = backup_to_s3
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.logic = MatchLogic()

    def safe_str(self, val):
        """Convert pandas value to string safely"""
        return "" if pd.isna(val) else str(val).strip()

    async def execute(self, file_bytes: BytesIO, filename: str) -> dict:
        # Save Excel temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes.read())
            tmp_path = tmp.name

        # Read Excel, skip template row
        df = pd.read_excel(tmp_path)
        df = df.iloc[2:]  # Skip header + template
        df = df.fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        vertices, edges = [], []

        for _, row in df.iterrows():
            # --- Input Part ---
            input_part = PartNumber(
                part_number=self.safe_str(row.get("Input Part Number")),
                spec1=self.safe_str(row.get("Input Spec 1")),
                spec2=self.safe_str(row.get("Input Spec 2")),
                spec3=self.safe_str(row.get("Input Spec 3")),
                spec4=self.safe_str(row.get("Input Spec 4")),
                spec5=self.safe_str(row.get("Input Spec 5")),
                note1=self.safe_str(row.get("Input Note 1")),
                note2=self.safe_str(row.get("Input Note 2")),
                note3=self.safe_str(row.get("Input Note 3")),
            )

            # --- Output Part ---
            output_part = PartNumber(
                part_number=self.safe_str(row.get("Output Part Number")),
                spec1=self.safe_str(row.get("Output Spec 1")),
                spec2=self.safe_str(row.get("Output Spec 2")),
                spec3=self.safe_str(row.get("Output Spec 3")),
                spec4=self.safe_str(row.get("Output Spec 4")),
                spec5=self.safe_str(row.get("Output Spec 5")),
                note1=self.safe_str(row.get("Output Note 1")),
                note2=self.safe_str(row.get("Output Note 2")),
                note3=self.safe_str(row.get("Output Note 3")),
            )

            # Save parts (async threads to avoid blocking)
            await asyncio.to_thread(self.part_usecase.create_part, input_part)
            await asyncio.to_thread(self.part_usecase.create_part, output_part)

            # --- Determine match type ---
            match_type = self.safe_str(row.get("Match Type")) or "AUTO"
            if match_type == "AUTO":
                match_type = self.logic.determine_match(
                    {f"spec{i}": getattr(input_part, f"spec{i}") for i in range(1, 6)},
                    {f"note{i}": getattr(input_part, f"note{i}") for i in range(1, 4)},
                    {f"spec{i}": getattr(output_part, f"spec{i}") for i in range(1, 6)},
                    {f"note{i}": getattr(output_part, f"note{i}") for i in range(1, 4)}
                )

            # --- Save match ---
            match = Match(input_part.part_number, output_part.part_number, match_type)
            await asyncio.to_thread(self.match_usecase.create_match, match)

            # Prepare vertices and edges for optional Neptune bulk loader
            vertices.extend([
                {
                    "~id": input_part.part_number,
                    "~label": "PartNumber",
                    "part_number": input_part.part_number,
                    "spec1": input_part.spec1,
                    "spec2": input_part.spec2,
                    "spec3": input_part.spec3,
                    "spec4": input_part.spec4,
                    "spec5": input_part.spec5,
                    "note1": input_part.note1,
                    "note2": input_part.note2,
                    "note3": input_part.note3,
                },
                {
                    "~id": output_part.part_number,
                    "~label": "PartNumber",
                    "part_number": output_part.part_number,
                    "spec1": output_part.spec1,
                    "spec2": output_part.spec2,
                    "spec3": output_part.spec3,
                    "spec4": output_part.spec4,
                    "spec5": output_part.spec5,
                    "note1": output_part.note1,
                    "note2": output_part.note2,
                    "note3": output_part.note3,
                }
            ])
            edges.append({
                "~from": input_part.part_number,
                "~to": output_part.part_number,
                "~label": "MATCHED",
                "match_type": match_type
            })

        # Convert to CSV & optional S3 backup
        vertices_df = pd.DataFrame(vertices).drop_duplicates("~id")
        edges_df = pd.DataFrame(edges)

        vertices_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        edges_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        vertices_df.to_csv(vertices_csv, index=False)
        edges_df.to_csv(edges_csv, index=False)

        if self.backup_to_s3 and self.s3_bucket:
            await upload_file_to_s3_async(vertices_csv, f"neptune_bulk/{os.path.basename(vertices_csv)}")
            await upload_file_to_s3_async(edges_csv, f"neptune_bulk/{os.path.basename(edges_csv)}")

        # Trigger Neptune bulk loader (optional)
        bulk_response = await trigger_bulk_load(f"s3://{self.s3_bucket}/neptune_bulk/", mode="NEW")
        bulk_load_id = bulk_response.get("payload", {}).get("loadId") if isinstance(bulk_response, dict) else None

        return {
            "message": "File processed successfully",
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "bulk_load_id": bulk_load_id
        }
