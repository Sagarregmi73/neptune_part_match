# lib/core/aws/neptune_bulk_loader.py

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

session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()

def trigger_bulk_load(s3_file_url: str, iam_role_arn: str):
    loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
    payload = {
        "source": s3_file_url,
        "format": "csv",
        "iamRoleArn": iam_role_arn,
        "region": AWS_REGION,
        "failOnError": "TRUE"
    }

    # Build AWS-signed request
    aws_request = AWSRequest(method="POST", url=loader_url, data=json.dumps(payload))
    SigV4Auth(credentials, "neptune-db", AWS_REGION).add_auth(aws_request)

    # Convert AWSRequest → requests
    prepared_request = requests.Request(
        method=aws_request.method,
        url=aws_request.url,
        headers=dict(aws_request.headers),
        data=aws_request.body
    ).prepare()

    session_requests = requests.Session()
    response = session_requests.send(prepared_request, verify=True)
    response.raise_for_status()
    return response.json()



'''import os
from lib.core.logging import logger
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import boto3
import requests
import json

load_dotenv(dotenv_path="env/local.env")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = os.getenv("NEPTUNE_PORT", 8182)
AWS_REGION = os.getenv("AWS_REGION")

session = boto3.session.Session()
credentials = session.get_credentials().get_frozen_credentials()

def trigger_bulk_load(s3_file_url: str,iam_role_arn: str):
    loader_url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/loader"
    payload = {
        "source": s3_file_url,
        "format": "csv",
        "iamRoleArn": iam_role_arn,
        "region": AWS_REGION,
        "failOnError": "TRUE"
    }

    request = AWSRequest(method="POST", url=loader_url, data=json.dumps(payload))
    SigV4Auth(credentials,"neptune-db",AWS_REGION).add_auth(request)

    headers = dict(request.headers)
    response = requests.post(loader_url, headers=headers, data=json.dumps(payload), verify=True)
    response.raise_for_status()
    return response.json()

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
        raise'''
