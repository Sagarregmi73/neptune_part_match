import os
import requests
import json

def trigger_bulk_load(s3_input_uri: str, mode: str = "NEW") -> dict:
    """
    Trigger Neptune Bulk Loader via HTTP API.
    
    :param s3_input_uri: S3 folder URI (e.g., s3://bucket/folder/)
    :param mode: NEW / RESUME / AUTO
    :return: dict containing Neptune loader response
    """
    # Get environment variables
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    region = os.getenv("AWS_REGION")

    if not all([neptune_endpoint, iam_role_arn, region]):
        raise Exception("NEPTUNE_ENDPOINT, NEPTUNE_IAM_ROLE_ARN, AWS_REGION must be set in environment variables")

    # Bulk loader API URL
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

    # Send POST request to Neptune Bulk Loader
    response = requests.post(loader_url, headers=headers, data=json.dumps(payload), verify=False)

    # Debug: print response for logs
    print("DEBUG Neptune Bulk Loader Response:", response.status_code, response.text)

    if response.status_code not in [200, 201]:
        raise Exception(f"Bulk load failed: {response.text}")

    return response.json()
