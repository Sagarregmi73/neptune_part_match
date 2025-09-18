import os
import requests
import json
import time

def trigger_bulk_load(s3_input_uri: str, mode: str = "NEW") -> dict:
    """
    Trigger Neptune Bulk Loader via HTTP API
    :param s3_input_uri: S3 folder URI (s3://bucket/key/)
    :param mode: NEW / RESUME / AUTO
    :return: dict containing 'loadId'
    """
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


def poll_bulk_load_status(load_id: str, interval: int = 5, timeout: int = 300) -> dict:
    """
    Poll Neptune Bulk Loader status until completion.
    :param load_id: Bulk Load ID returned by trigger_bulk_load
    :param interval: Polling interval in seconds
    :param timeout: Maximum wait time in seconds
    :return: dict with 'status' and 'failures'
    """
    if not load_id:
        raise ValueError("load_id must be provided to poll status")

    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    if not neptune_endpoint:
        raise Exception("NEPTUNE_ENDPOINT must be set in env")

    status_url = f"https://{neptune_endpoint}:8182/loader/{load_id}"
    headers = {"Content-Type": "application/json"}

    start_time = time.time()
    while True:
        response = requests.get(status_url, headers=headers, verify=False)
        if response.status_code not in [200, 201]:
            raise Exception(f"Failed to poll bulk load: {response.text}")

        result = response.json()
        status = result.get("status")
        failures = result.get("failures", [])

        if status in ["LOAD_COMPLETED", "LOAD_FAILED", "LOAD_CANCELLED"]:
            return {"status": status, "failures": failures}

        if time.time() - start_time > timeout:
            raise TimeoutError(f"Polling bulk load timed out after {timeout} seconds")

        time.sleep(interval)
