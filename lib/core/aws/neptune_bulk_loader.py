# lib/core/aws/neptune_bulk_loader.py
import os
import json
import time
import boto3
import requests
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import NoCredentialsError, ClientError

load_dotenv(dotenv_path="env/local.env")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = os.getenv("NEPTUNE_PORT", "8182")
AWS_REGION = os.getenv("AWS_REGION")

# boto3 clients
session = boto3.session.Session()
credentials_raw = session.get_credentials()
if credentials_raw is None:
    raise NoCredentialsError("No AWS credentials found (session.get_credentials() returned None). Ensure EC2 role or env credentials are configured.")
credentials = credentials_raw.get_frozen_credentials()
s3_client = boto3.client("s3", region_name=AWS_REGION)

def _sigv4_signed_request(method: str, url: str, json_body: dict):
    """
    Build a SigV4-signed requests.PreparedRequest for Neptune.
    """
    aws_request = AWSRequest(method=method, url=url, data=json.dumps(json_body))
    SigV4Auth(credentials, "neptune-db", AWS_REGION).add_auth(aws_request)

    prepared_request = requests.Request(
        method=aws_request.method,
        url=aws_request.url,
        headers=dict(aws_request.headers),
        data=aws_request.body
    ).prepare()
    return prepared_request

def trigger_bulk_load(s3_path: str, iam_role_arn: str, wait_for_head: bool = True, head_retries: int = 5, head_backoff: float = 1.0):
    """
    Trigger Neptune bulk load for a specific S3 file or for all CSV files in a prefix.
    s3_path may be:
      - 's3://bucket/key.csv'  (single file)
      - 's3://bucket/prefix/'  (folder/prefix) - will trigger for each CSV under prefix
    Returns list of responses (each is Neptune JSON) keyed by s3 key.
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("s3_path must start with s3://")

    # parse bucket and prefix/key
    path_parts = s3_path[5:].split("/", 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""

    # list objects for the prefix (if s3_path is a folder) or test single file
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    if "Contents" not in objects:
        # If user passed exact file name and not found, raise
        raise ValueError(f"No objects found in s3://{bucket}/{prefix}")

    responses = []
    for obj in objects["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".csv"):
            continue

        s3_url = f"s3://{bucket}/{key}"

        # optionally wait until the object is readable via head_object (helps eventual consistency)
        if wait_for_head:
            head_ok = False
            for attempt in range(head_retries):
                try:
                    s3_client.head_object(Bucket=bucket, Key=key)
                    head_ok = True
                    break
                except ClientError as e:
                    # Not found yet, wait and retry
                    time.sleep(head_backoff * (attempt+1))
            if not head_ok:
                raise RuntimeError(f"S3 object {s3_url} not visible after {head_retries} attempts")

        payload = {
            "source": s3_url,
            "format": "csv",
            "iamRoleArn": iam_role_arn,
            "region": AWS_REGION,
            "failOnError": True
        }

        loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"

        # Debug log
        print("Triggering Neptune bulk load with payload:")
        print(json.dumps(payload, indent=2))

        prepared_request = _sigv4_signed_request("POST", loader_url, payload)

        with requests.Session() as session_requests:
            response = session_requests.send(prepared_request, verify=True)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                # Attach response body for diagnostics
                text = response.text if response is not None else str(e)
                print(f"Neptune loader HTTP error for {s3_url}: {text}")
                # raise a clear error up to caller
                raise RuntimeError(f"Neptune loader returned HTTP {response.status_code}: {text}") from e

            try:
                resp_json = response.json()
            except ValueError:
                resp_json = {"raw": response.text}
            responses.append({key: resp_json})

    return responses
