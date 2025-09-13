import os
import requests
import json

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = os.getenv("NEPTUNE_PORT", "8182")
NEPTUNE_REGION = os.getenv("AWS_REGION")
IAM_ROLE_ARN = os.getenv("NEPTUNE_IAM_ROLE_ARN")

def start_bulk_load(s3_path: str, data_format="csv"):
    """
    Trigger Neptune bulk loader for a given S3 file or folder.
    """
    if not IAM_ROLE_ARN:
        raise Exception("NEPTUNE_IAM_ROLE_ARN is not set in environment")

    loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"

    payload = {
        "source": s3_path,
        "format": data_format,
        "iamRoleArn": IAM_ROLE_ARN,
        "region": NEPTUNE_REGION,
        "failOnError": "FALSE",
        "parallelism": "LOW",
        "updateSingleCardinalityProperties": "FALSE",
        "queueRequest": "TRUE"
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(loader_url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        raise Exception(f"Failed to start bulk load: {response.text}")

    return response.json()
