import pandas as pd
import tempfile
import os
from io import BytesIO
import aiofiles
from lib.core.aws.s3_client import upload_file_to_s3_async
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.app.domain.services.match_logic import MatchLogic
from lib.app.domain.entities.part_number import PartNumber
from lib.app.domain.entities.match import Match

class UploadFileUseCase:
    def __init__(self, part_usecase, match_usecase, backup_to_s3=True):
        self.part_usecase = part_usecase  # now sync
        self.match_usecase = match_usecase  # now sync
        self.backup_to_s3 = backup_to_s3
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.logic = MatchLogic()

    async def execute(self, file_bytes: BytesIO, filename: str) -> dict:
        # Save uploaded file asynchronously
        async with aiofiles.tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            await tmp.write(file_bytes.read())
            tmp_path = tmp.name

        df = pd.read_excel(tmp_path)
        df = df.iloc[2:]  # skip template rows

        vertices, edges = [], []

        for _, row in df.iterrows():
            input_part = PartNumber(
                row.get("Input Part Number"),
                row.get("Input Spec 1"),
                row.get("Input Spec 2"),
                row.get("Input Spec 3"),
                row.get("Input Spec 4"),
                row.get("Input Spec 5"),
                row.get("Input Note 1", ""),
                row.get("Input Note 2", ""),
                row.get("Input Note 3", "")
            )
            output_part = PartNumber(
                row.get("Output Part Number"),
                row.get("Output Spec 1"),
                row.get("Output Spec 2"),
                row.get("Output Spec 3"),
                row.get("Output Spec 4"),
                row.get("Output Spec 5"),
                row.get("Output Note 1", ""),
                row.get("Output Note 2", ""),
                row.get("Output Note 3", "")
            )

            # Sync save parts
            self.part_usecase.create_part(input_part)
            self.part_usecase.create_part(output_part)

            # Determine match type automatically
            match_type = row.get("Match Type")
            if not match_type or str(match_type).lower() in ["", "auto"]:
                match_type = self.logic.determine_match(
                    {f"spec{i+1}": getattr(input_part, f"spec{i+1}") for i in range(5)},
                    {f"note{i+1}": getattr(input_part, f"note{i+1}") for i in range(3)},
                    {f"spec{i+1}": getattr(output_part, f"spec{i+1}") for i in range(5)},
                    {f"note{i+1}": getattr(output_part, f"note{i+1}") for i in range(3)}
                )

            match = Match(input_part.part_number, output_part.part_number, match_type)
            self.match_usecase.create_match(match)  # sync

            # Prepare CSV for Neptune bulk loader
            vertices.append({
                "~id": input_part.part_number, "~label": "PartNumber",
                **{f"spec{i+1}": getattr(input_part, f"spec{i+1}") for i in range(5)},
                **{f"note{i+1}": getattr(input_part, f"note{i+1}") for i in range(3)}
            })
            vertices.append({
                "~id": output_part.part_number, "~label": "PartNumber",
                **{f"spec{i+1}": getattr(output_part, f"spec{i+1}") for i in range(5)},
                **{f"note{i+1}": getattr(output_part, f"note{i+1}") for i in range(3)}
            })
            edges.append({
                "~from": input_part.part_number, "~to": output_part.part_number,
                "~label": "MATCHED", "match_type": match_type
            })

        # Save CSVs
        vertices_df = pd.DataFrame(vertices).drop_duplicates(subset="~id")
        edges_df = pd.DataFrame(edges)

        vertices_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        edges_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv").name
        vertices_df.to_csv(vertices_csv, index=False)
        edges_df.to_csv(edges_csv, index=False)

        if self.backup_to_s3:
            await upload_file_to_s3_async(vertices_csv, f"neptune_bulk/{os.path.basename(vertices_csv)}")
            await upload_file_to_s3_async(edges_csv, f"neptune_bulk/{os.path.basename(edges_csv)}")

        # Trigger Neptune bulk load
        bulk_response = await trigger_bulk_load(f"s3://{self.s3_bucket}/neptune_bulk/", mode="NEW")
        bulk_load_id = bulk_response.get("payload", {}).get("loadId") if isinstance(bulk_response, dict) else None

        return {
            "message": "File processed and matches created successfully",
            "vertices_created": len(vertices_df),
            "edges_created": len(edges_df),
            "bulk_load_id": bulk_load_id
        }
