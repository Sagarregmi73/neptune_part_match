import pandas as pd
import tempfile, os, uuid
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

        # Combine input specs/notes as single strings
        for row in df.itertuples(index=False):
            input_part = getattr(row, "Input_Part_Number", None)
            output_part = getattr(row, "Output_Part_Number", None)
            input_specs = ",".join([str(getattr(row, f"Input Spec {i}", "")) for i in range(1,6)])
            input_notes = ",".join([str(getattr(row, f"Input Note {i}", "")) for i in range(1,4)])
            output_specs = ",".join([str(getattr(row, f"Output Spec {i}", "")) for i in range(1,6)])
            output_notes = ",".join([str(getattr(row, f"Output Note {i}", "")) for i in range(1,4)])
            match_type_raw = getattr(row, "Match Type", None)

            # Create input vertex
            if input_part and input_part not in vertices_created:
                self.repo.create_part(PartNumber(input_part, input_specs, input_notes))
                vertices_created.add(input_part)

            # Create output vertex
            if output_part and output_part != "-" and output_part not in vertices_created:
                self.repo.create_part(PartNumber(output_part, output_specs, output_notes))
                vertices_created.add(output_part)

            # Create edge
            if output_part and output_part != "-" and match_type_raw:
                match_type = "Replacement" if match_type_raw in ["Perfect", "Partial"] else "No Replacement"
                self.repo.create_match(Match(input_part, output_part, match_type))
                edges_created += 1

        # Optional S3 backup
        if self.backup_to_s3:
            job_id = str(uuid.uuid4())
            job_prefix = f"bulk_load/{job_id}/"
            vertices_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            edges_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

            # vertices.csv
            vertices_data = []
            for row in df.itertuples(index=False):
                for part, specs, notes in [
                    (getattr(row, "Input_Part_Number", None), ",".join([str(getattr(row, f"Input Spec {i}", "")) for i in range(1,6)]),
                     ",".join([str(getattr(row, f"Input Note {i}", "")) for i in range(1,4)])),
                    (getattr(row, "Output_Part_Number", None), ",".join([str(getattr(row, f"Output Spec {i}", "")) for i in range(1,6)]),
                     ",".join([str(getattr(row, f"Output Note {i}", "")) for i in range(1,4)]))
                ]:
                    if part and part != "-":
                        vertices_data.append({"~id": part, "~label":"Part", "part_number:String": part, "specs:String": specs, "notes:String": notes})
            pd.DataFrame(vertices_data).drop_duplicates(subset="~id").to_csv(vertices_file.name, index=False)

            # edges.csv
            edges_file.write(b"~from,~to,~label,match_type:String\n")
            for row in df.itertuples(index=False):
                input_part = getattr(row, "Input_Part_Number", None)
                output_part = getattr(row, "Output_Part_Number", None)
                match_type_raw = getattr(row, "Match Type", None)
                if input_part and output_part and output_part != "-" and match_type_raw:
                    match_type = "Replacement" if match_type_raw in ["Perfect", "Partial"] else "No Replacement"
                    edges_file.write(f"{input_part},{output_part},Match,{match_type}\n".encode())
            edges_file.close()

            # Upload
            vertices_s3 = upload_file_to_s3(vertices_file.name, f"{job_prefix}vertices.csv")
            edges_s3 = upload_file_to_s3(edges_file.name, f"{job_prefix}edges.csv")
            bucket_name = os.getenv("S3_BUCKET_NAME")
            s3_folder = f"s3://{bucket_name}/{job_prefix}"
            loader_results = trigger_bulk_load(s3_folder)

            # Cleanup
            os.remove(vertices_file.name)
            os.remove(edges_file.name)

            return {"status":"success","vertices_count":len(vertices_created),"edges_count":edges_created,
                    "vertices_s3":vertices_s3,"edges_s3":edges_s3,"s3_folder":s3_folder,"loader_results":loader_results}

        return {"status":"success","vertices_created":len(vertices_created),"edges_created":edges_created}
