# lib/app/application/services/file_service.py
import os
import pandas as pd
from lib.core.aws.s3_client import upload_file
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.logging import logger
from lib.core.utils.config_loader import NEPTUNE_IAM_ROLE_ARN  # ensure defined

class FileService:
    def __init__(self, temp_dir: str = "/tmp"):
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

    def process_file(self, file_obj, filename: str):
        """
        Process uploaded Excel: save locally, generate vertices/edges CSV,
        upload to S3, and trigger Neptune bulk load.
        """
        local_path = os.path.join(self.temp_dir, filename)
        with open(local_path, "wb") as f:
            f.write(file_obj.read())
        logger.info(f"Saved file locally: {local_path}")

        # Parse Excel
        df = pd.read_excel(local_path)
        vertices_csv, edges_csv = self._generate_csv(df)

        # Upload CSVs to S3
        vertices_key = f"uploads/{os.path.basename(vertices_csv)}"
        edges_key = f"uploads/{os.path.basename(edges_csv)}"

        vertices_s3 = upload_file(vertices_csv, vertices_key)
        edges_s3 = upload_file(edges_csv, edges_key)

        logger.info(f"Uploaded vertices CSV to {vertices_s3}")
        logger.info(f"Uploaded edges CSV to {edges_s3}")

        # Trigger Neptune bulk loader (vertices first)
        loader_response_vertices = trigger_bulk_load(vertices_s3, NEPTUNE_IAM_ROLE_ARN)
        loader_response_edges = trigger_bulk_load(edges_s3, NEPTUNE_IAM_ROLE_ARN)

        return {
            "vertices_s3": vertices_s3,
            "edges_s3": edges_s3,
            "loader_response_vertices": loader_response_vertices,
            "loader_response_edges": loader_response_edges,
        }

    def _generate_csv(self, df):
        """
        Convert Excel rows to Neptune-compatible vertices and edges CSV.
        """
        vertices_csv = os.path.join(self.temp_dir, "vertices.csv")
        edges_csv = os.path.join(self.temp_dir, "edges.csv")

        # Build vertices (~id, ~label first)
        df_vertices = df[["Input Part Number", "Input Specs", "Input Notes"]].rename(
            columns={
                "Input Part Number": "~id",
                "Input Specs": "specs",
                "Input Notes": "notes",
            }
        )
        df_vertices["~label"] = "PartNumber"
        df_vertices = df_vertices[["~id", "~label", "specs", "notes"]]
        df_vertices = df_vertices.dropna(subset=["~id"])  # remove blanks
        df_vertices.to_csv(vertices_csv, index=False)

        # Build edges (~id, ~from, ~to, ~label first)
        edges = []
        for _, row in df.iterrows():
            if row.get("Output Part Number") and row["Output Part Number"] != "-":
                edges.append({
                    "~id": f"{row['Input Part Number']}_to_{row['Output Part Number']}",
                    "~from": row["Input Part Number"],
                    "~to": row["Output Part Number"],
                    "~label": "replacement",
                    "match_type": row["Match Type"],
                })
        df_edges = pd.DataFrame(edges)
        if not df_edges.empty:
            df_edges = df_edges[["~id", "~from", "~to", "~label", "match_type"]]
            df_edges.to_csv(edges_csv, index=False)

        return vertices_csv, edges_csv
