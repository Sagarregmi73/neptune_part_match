# lib/core/aws/neptune_bulk_loader.py
import requests
import os
from lib.core.logging import logger
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/local.env")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = os.getenv("NEPTUNE_PORT", 8182)

def trigger_bulk_load(s3_file_url: str, iam_role_arn: str, region: str = "us-east-1"):
    """
    s3_file_url: Full S3 URL of CSV/JSON file for Neptune bulk loader
    iam_role_arn: IAM role ARN that Neptune can assume
    """
    loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
    payload = {
        "source": s3_file_url,
        "format": "csv",
        "iamRoleArn": iam_role_arn,
        "region": region,
        "failOnError": "TRUE"
    }
    try:
        response = requests.post(loader_url, json=payload)
        response.raise_for_status()
        logger.info(f"Bulk load triggered successfully: {response.json()}")
        return response.json()
    except Exception as e:
        logger.error(f"Failed to trigger bulk loader: {e}")
        raise
