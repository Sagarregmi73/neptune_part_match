import pandas as pd
import tempfile
from io import BytesIO
import asyncio
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match
from lib.app.domain.services.match_logic import MatchLogic
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.aws.s3_client import upload_file_to_s3_async
import os

class UploadFileUseCase:
    def __init__(self, part_usecase, match_usecase, backup_to_s3=True):
        self.part_usecase = part_usecase
        self.match_usecase = match_usecase
        self.backup_to_s3 = backup_to_s3
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.logic = MatchLogic()

    def safe_str(self, val):
        return "" if pd.isna(val) else str(val).strip()

    async def execute(self, file_bytes: BytesIO, filename: str) -> dict:
        # Save Excel temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(file_bytes.read())
            tmp_path = tmp.name

        # Read Excel: skip template row
        df = pd.read_excel(tmp_path)
        df = df.iloc[2:]  # skip header + template
        df = df.fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        vertices, edges = [], []

        for _, row in df.iterrows():
            input_part = PartNumber(
                self.safe_str(row.get("Input Part Number")),
                *[self.safe_str(row.get(f"Input Spec {i}")) for i in range(1,6)],
                *[self.safe_str(row.get(f"Input Note {i}")) for i in range(1,4)]
            )

            output_part = PartNumber(
                self.safe_str(row.get("Output Part Number")),
                *[self.safe_str(row.get(f"Output Spec {i}")) for i in range(1,6)],
                *[self.safe_str(row.get(f"Output Note {i}")) for i in range(1,4)]
            )

            # Save parts
            await asyncio.to_thread(self.part_usecase.create_part, input_part)
            await asyncio.to_thread(self.part_usecase.create_part, output_part)

            # Determine match type
            match_type = self.safe_str(row.get("Match Type")) or "AUTO"
            if match_type == "AUTO":
                match_type = self.logic.determine_match(
                    {f"spec{i}": getattr(input_part, f"spec{i}") for i in range(1,6)},
                    {f"note{i}": getattr(input_part, f"note{i}") for i in range(1,4)},
                    {f"spec{i}": getattr(output_part, f"spec{i}") for i in range(1,6)},
                    {f"note{i}": getattr(output_part, f"note{i}") for i in range(1,4)}
                )

            match = Match(input_part.part_number, output_part.part_number, match_type)
            await asyncio.to_thread(self.match_usecase.create_match, match)

            # Prepare for bulk loader
            vertices.extend([
                {"~id": input_part.part_number, "~label": "PartNumber", **{f"spec{i}": getattr(input_part, f"spec{i}") for i in range(1,6)}, **{f"note{i}": getattr(input_part, f"note{i}") for i in range(1,4)}},
                {"~id": output_part.part_number, "~label": "PartNumber", **{f"spec{i}": getattr(output_part, f"spec{i}") for i in range(1,6)}, **{f"note{i}": getattr(output_part, f"note{i}") for i in range(1,4)}}
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

        if self.backup_to_s3:
            await upload_file_to_s3_async(vertices_csv, f"neptune_bulk/{os.path.basename(vertices_csv)}")
            await upload_file_to_s3_async(edges_csv, f"neptune_bulk/{os.path.basename(edges_csv)}")

        bulk_response = await trigger_bulk_load(f"s3://{self.s3_bucket}/neptune_bulk/", mode="NEW")
        bulk_load_id = bulk_response.get("payload", {}).get("loadId") if isinstance(bulk_response, dict) else None

        return {
            "message": "File processed successfully",
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "bulk_load_id": bulk_load_id
        }
