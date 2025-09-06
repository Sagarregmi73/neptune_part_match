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

def trigger_bulk_load(s3_file_url: str, iam_role_arn: str):
    """
    Triggers a Neptune bulk load job from an S3 file.
    """
    loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"

    payload = {
        "source": s3_file_url,
        "format": "csv",  # or "json"/"ntriples" depending on your data
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
        return response.json()
