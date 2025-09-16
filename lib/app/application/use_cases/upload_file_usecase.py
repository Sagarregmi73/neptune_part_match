import pandas as pd
import tempfile
import os
import uuid
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

        # Normal repo persistence (optional)
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

        # Bulk load prep
        if self.backup_to_s3:
            job_id = str(uuid.uuid4())
            job_prefix = f"bulk_load/{job_id}/"

            vertices_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            edges_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

            # Build vertices.csv (Neptune bulk load format)
            vertices_data = []
            for row in df.itertuples():
                if getattr(row, "Input_Part_Number", None):
                    vertices_data.append({
                        "~id": row.Input_Part_Number,
                        "~label": "Part",
                        "part_number:String": row.Input_Part_Number,
                        "specs:String": getattr(row, "Input_Specs", ""),
                        "notes:String": getattr(row, "Input_Notes", "")
                    })
                if getattr(row, "Output_Part_Number", None) and row.Output_Part_Number != "-":
                    vertices_data.append({
                        "~id": row.Output_Part_Number,
                        "~label": "Part",
                        "part_number:String": row.Output_Part_Number,
                        "specs:String": getattr(row, "Output_Specs", ""),
                        "notes:String": getattr(row, "Output_Notes", "")
                    })

            pd.DataFrame(vertices_data).drop_duplicates(subset="~id") \
                .to_csv(vertices_file.name, index=False)

            # Build edges.csv (Neptune bulk load format)
            edges_file.write(b"~from,~to,~label,match_type:String\n")
            for row in df.itertuples():
                if getattr(row, "Match_Type", None) and row.Output_Part_Number and row.Output_Part_Number != "-":
                    match_type = "Replacement" if row.Match_Type in ["Perfect", "Partial"] else "No Replacement"
                    edges_file.write(
                        f"{row.Input_Part_Number},{row.Output_Part_Number},Match,{match_type}\n".encode()
                    )
            edges_file.close()

            # Upload to S3
            vertices_s3 = upload_file_to_s3(vertices_file.name, f"{job_prefix}vertices.csv")
            edges_s3 = upload_file_to_s3(edges_file.name, f"{job_prefix}edges.csv")

            # Trigger bulk load on **folder**, not individual files
            bucket_name = os.getenv("S3_BUCKET_NAME")
            s3_folder = f"s3://{bucket_name}/{job_prefix}"
            loader_results = trigger_bulk_load(s3_folder)

            # Cleanup temp files
            os.remove(vertices_file.name)
            os.remove(edges_file.name)

            return {
                "status": "success",
                "vertices_s3": vertices_s3,
                "edges_s3": edges_s3,
                "s3_folder": s3_folder,
                "loader_results": loader_results
            }

        return {"status": "success", "vertices_created": len(vertices_created), "edges_created": edges_created}
