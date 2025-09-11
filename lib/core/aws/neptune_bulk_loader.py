import os
import json
import boto3
import requests
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

load_dotenv(dotenv_path="env/local.env")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = os.getenv("NEPTUNE_PORT", "8182")
AWS_REGION = os.getenv("AWS_REGION")

# Get AWS credentials
session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()
s3_client = boto3.client("s3", region_name=AWS_REGION)

def trigger_bulk_load(s3_path: str, iam_role_arn: str):
    """
    Triggers Neptune bulk load for all CSV files in the given S3 path.
    
    Args:
        s3_path: S3 folder or file, e.g. s3://bucket/uploads/
        iam_role_arn: IAM role ARN for Neptune to access S3
    Returns:
        List of load responses from Neptune
    """
    # Parse bucket and prefix from s3_path
    if not s3_path.startswith("s3://"):
        raise ValueError("s3_path must start with s3://")
    
    path_parts = s3_path[5:].split("/", 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""
    
    # List all CSV files under this prefix
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in objects:
        raise ValueError(f"No files found in S3 path: {s3_path}")
    
    responses = []
    for obj in objects["Contents"]:
        key = obj["Key"]
        if not key.endswith(".csv"):
            continue
        
        loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
        payload = {
            "source": f"s3://{bucket}/{key}",
            "format": "csv",
            "iamRoleArn": iam_role_arn,
            "region": AWS_REGION,
            "failOnError": "TRUE"
        }
        
        # Build SigV4 signed request
        aws_request = AWSRequest(method="POST", url=loader_url, data=json.dumps(payload))
        SigV4Auth(credentials, "neptune-db", AWS_REGION).add_auth(aws_request)
        
        # Convert AWSRequest to requests.PreparedRequest
        prepared_request = requests.Request(
            method=aws_request.method,
            url=aws_request.url,
            headers=dict(aws_request.headers),
            data=aws_request.body
        ).prepare()
        
        # Send request
        with requests.Session() as session_requests:
            response = session_requests.send(prepared_request, verify=True)
            response.raise_for_status()
            responses.append({key: response.json()})
    
    return responses
