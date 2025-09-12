# lib/app/application/services/file_service.py
import os
import pandas as pd
from lib.core.aws.s3_client import upload_file
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.logging import logger
from lib.core.utils.config_loader import NEPTUNE_IAM_ROLE_ARN  # make sure this is defined

class FileService:
    def __init__(self, temp_dir: str = "/tmp"):
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

    def process_file(self, file_obj, filename: str):
        """
        Process uploaded Excel/CSV: save locally, generate nodes/edges CSV,
        upload to S3, and trigger Neptune bulk load.
        """
        local_path = os.path.join(self.temp_dir, filename)
        with open(local_path, "wb") as f:
            f.write(file_obj.read())
        logger.info(f"Saved file locally: {local_path}")

        # Parse Excel to nodes/edges CSV
        df = pd.read_excel(local_path)
        nodes_csv, edges_csv = self._generate_csv(df)

        # Upload CSVs to S3
        nodes_key = f"nodes/{os.path.basename(nodes_csv)}"
        edges_key = f"edges/{os.path.basename(edges_csv)}"

        nodes_s3 = upload_file(nodes_csv, nodes_key)  # should return s3://bucket/key
        edges_s3 = upload_file(edges_csv, edges_key)

        logger.info(f"Uploaded nodes CSV to {nodes_s3}")
        logger.info(f"Uploaded edges CSV to {edges_s3}")

        # Trigger Neptune bulk loader for nodes first, then edges
        loader_response_nodes = trigger_bulk_load(nodes_s3, NEPTUNE_IAM_ROLE_ARN)
        loader_response_edges = trigger_bulk_load(edges_s3, NEPTUNE_IAM_ROLE_ARN)

        return {
            "nodes_s3": nodes_s3,
            "edges_s3": edges_s3,
            "loader_response_nodes": loader_response_nodes,
            "loader_response_edges": loader_response_edges,
        }

    def _generate_csv(self, df):
        """
        Convert Excel rows to nodes and edges CSV for Neptune.
        """
        nodes_csv = os.path.join(self.temp_dir, "nodes.csv")
        edges_csv = os.path.join(self.temp_dir, "edges.csv")

        # Build nodes
        df_nodes = df[["Input Part Number", "Input Specs", "Input Notes"]].rename(
            columns={
                "Input Part Number": "~id",
                "Input Specs": "specs",
                "Input Notes": "notes",
            }
        )
        df_nodes["~label"] = "PartNumber"  # add label column for Neptune
        df_nodes.to_csv(nodes_csv, index=False)

        # Build edges
        edges = []
        for _, row in df.iterrows():
            if row["Output Part Number"] != "-":
                edges.append({
                    "~id": f"{row['Input Part Number']}_to_{row['Output Part Number']}",
                    "~from": row["Input Part Number"],
                    "~to": row["Output Part Number"],
                    "~label": "replacement",
                    "match_type": row["Match Type"],
                })
        pd.DataFrame(edges).to_csv(edges_csv, index=False)

        return nodes_csv, edges_csv
