import os
import requests
import json

def trigger_bulk_load(s3_input_uri: str, mode: str = "NEW") -> dict:
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    region = os.getenv("AWS_REGION")

    if not all([neptune_endpoint, iam_role_arn, region]):
        raise Exception("NEPTUNE_ENDPOINT, NEPTUNE_IAM_ROLE_ARN, AWS_REGION must be set in env")

    loader_url = f"https://{neptune_endpoint}:8182/loader"

    payload = {
        "source": s3_input_uri,
        "format": "csv",
        "iamRoleArn": iam_role_arn,
        "region": region,
        "mode": mode,
        "failOnError": True,
        "parallelism": "MEDIUM",
        "updateSingleCardinalityProperties": True
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(loader_url, headers=headers, data=json.dumps(payload), verify=False)
    if response.status_code not in [200, 201]:
        raise Exception(f"Bulk load failed: {response.text}")

    return response.json()
