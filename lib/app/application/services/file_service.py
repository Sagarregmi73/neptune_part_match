import pandas as pd
import tempfile
from lib.core.aws.s3_client import upload_file_to_s3
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load

class FileService:
    def execute(self, file_bytes: bytes, filename: str):
        df = pd.read_excel(file_bytes)
        vertices_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        edges_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")

        # For demo: simply write the excel to vertices.csv
        df.to_csv(vertices_file.name, index=False)
        edges_file.write(b"source,target,match_type\n")  # empty edges for demo
        edges_file.close()

        vertices_s3 = upload_file_to_s3(vertices_file.name, f"vertices/{filename}.csv")
        edges_s3 = upload_file_to_s3(edges_file.name, f"edges/{filename}.csv")

        trigger_bulk_load(vertices_s3, edges_s3)
        return {"vertices_s3": vertices_s3, "edges_s3": edges_s3}
