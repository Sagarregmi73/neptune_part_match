import os
import boto3
import requests
import socket
import ssl
from botocore.exceptions import BotoCoreError, ClientError

def trigger_bulk_load(s3_input_uri: str, mode: str = "RESUME") -> dict:
    """
    Trigger a Neptune bulk load job safely, handling VPC and endpoint issues.

    :param s3_input_uri: The S3 path where vertices/edges CSV files are uploaded.
    :param mode: LOAD | RESUME | NEW (default: RESUME)
    :return: The Neptune bulk loader response
    """
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")  # cluster HTTPS endpoint
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    s3_bucket_region = os.getenv("AWS_REGION")

    # Validate environment variables
    if not neptune_endpoint:
        raise Exception("Environment variable NEPTUNE_ENDPOINT is not set.")
    if not iam_role_arn:
        raise Exception("Environment variable NEPTUNE_IAM_ROLE_ARN is not set.")
    if not s3_bucket_region:
        raise Exception("Environment variable AWS_REGION is not set.")

    # Verify endpoint DNS resolution
    try:
        socket.gethostbyname(neptune_endpoint)
        print(f"DNS resolution for {neptune_endpoint} successful")
    except socket.gaierror:
        raise Exception(f"Cannot resolve Neptune endpoint '{neptune_endpoint}'.")

    # Check HTTPS connectivity
    try:
        context = ssl.create_default_context()
        with socket.create_connection((neptune_endpoint, 8182), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=neptune_endpoint):
                print(f"Neptune endpoint {neptune_endpoint}:8182 reachable over HTTPS")
    except Exception as e:
        raise Exception(f"Cannot reach Neptune endpoint '{neptune_endpoint}:8182'. Original error: {e}")

    # Initialize Neptune Data client
    client = boto3.client("neptunedata", region_name=s3_bucket_region, endpoint_url=f"https://{neptune_endpoint}:8182")

    # Trigger bulk load
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
        print("Neptune bulk load started successfully")
        return response
    except (BotoCoreError, ClientError) as e:
        raise Exception(f"Failed to start Neptune bulk load. Error: {e}")
