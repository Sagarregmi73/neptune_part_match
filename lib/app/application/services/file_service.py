# lib/app/application/services/file_service.py
import os
import csv
import pandas as pd
from lib.core.aws.s3_client import upload_file, download_file
from lib.core.aws.neptune_bulk_loader import trigger_bulk_load
from lib.core.logging import logger

class FileService:
    def __init__(self, temp_dir: str = "/tmp"):
        self.temp_dir = temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

    def process_file(self, file_obj, filename: str):
        """Process uploaded Excel/CSV: save locally, generate CSV, upload to S3, call Neptune"""
        local_path = os.path.join(self.temp_dir, filename)
        with open(local_path, "wb") as f:
            f.write(file_obj.read())
        logger.info(f"Saved file locally: {local_path}")

        # Parse Excel to nodes/edges CSV
        df = pd.read_excel(local_path)
        nodes_csv, edges_csv = self._generate_csv(df)

        # Upload CSV to S3
        nodes_s3 = upload_file(nodes_csv, f"nodes/{os.path.basename(nodes_csv)}")
        edges_s3 = upload_file(edges_csv, f"edges/{os.path.basename(edges_csv)}")

        # Trigger Neptune bulk loader
        trigger_bulk_load(nodes_s3, edges_s3)
        return {"nodes_s3": nodes_s3, "edges_s3": edges_s3}

    def _generate_csv(self, df):
        """Convert Excel rows to nodes and edges CSV for Neptune"""
        nodes_csv = os.path.join(self.temp_dir, "nodes.csv")
        edges_csv = os.path.join(self.temp_dir, "edges.csv")

        # Nodes CSV
        df_nodes = df[['Input Part Number', 'Input Specs', 'Input Notes']].rename(
            columns={'Input Part Number': 'id', 'Input Specs': 'specs', 'Input Notes': 'notes'}
        )
        df_nodes.to_csv(nodes_csv, index=False)

        # Edges CSV
        edges = []
        for _, row in df.iterrows():
            if row['Output Part Number'] != "-":
                edges.append({
                    'source': row['Input Part Number'],
                    'target': row['Output Part Number'],
                    'match_type': row['Match Type']
                })
        pd.DataFrame(edges).to_csv(edges_csv, index=False)
        return nodes_csv, edges_csv
