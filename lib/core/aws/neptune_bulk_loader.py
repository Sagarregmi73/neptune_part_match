# lib/app/application/use_cases/bulk_loader_trigger.py

import os
import socket
import ssl
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def trigger_bulk_load(s3_input_uri: str, mode: str = "RESUME") -> dict:
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    s3_bucket_region = os.getenv("AWS_REGION")

    if not all([neptune_endpoint, iam_role_arn, s3_bucket_region]):
        raise Exception("NEPTUNE_ENDPOINT, NEPTUNE_IAM_ROLE_ARN, AWS_REGION must be set")

    # DNS & HTTPS check
    try:
        socket.gethostbyname(neptune_endpoint)
    except socket.gaierror:
        raise Exception(f"Cannot resolve Neptune endpoint '{neptune_endpoint}'.")

    try:
        context = ssl.create_default_context()
        with socket.create_connection((neptune_endpoint, 8182), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=neptune_endpoint):
                pass
    except Exception as e:
        raise Exception(f"Cannot reach Neptune endpoint '{neptune_endpoint}:8182'. Original error: {e}")

    client = boto3.client(
        "neptunedata",
        region_name=s3_bucket_region,
        endpoint_url=f"https://{neptune_endpoint}:8182"
    )

    try:
        response = client.start_loader_job(
            source=s3_input_uri,
            format="csv",
            iamRoleArn=iam_role_arn,
            s3BucketRegion=s3_bucket_region,
            mode=mode,
            failOnError=True,
            parallelism="MEDIUM",
            updateSingleCardinalityProperties=True
        )
        return response
    except (BotoCoreError, ClientError) as e:
        raise Exception(f"Failed to start Neptune bulk load. Error: {e}")
