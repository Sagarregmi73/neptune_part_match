import os
import boto3
import socket
import ssl
from botocore.exceptions import BotoCoreError, ClientError

def trigger_bulk_load(s3_input_uri: str, mode: str = "RESUME") -> dict:
    """
    Trigger a Neptune bulk load job safely with HTTPS connectivity checks.

    :param s3_input_uri: The S3 path where vertices/edges CSV files are uploaded.
    :param mode: LOAD | RESUME | NEW (default: RESUME)
    :return: The Neptune bulk loader response
    """
    # Load environment variables
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    s3_bucket_region = os.getenv("AWS_REGION")  # S3 bucket region

    # Validate environment variables
    if not neptune_endpoint:
        raise Exception("Environment variable NEPTUNE_ENDPOINT is not set.")
    if not iam_role_arn:
        raise Exception("Environment variable NEPTUNE_IAM_ROLE_ARN is not set.")
    if not s3_bucket_region:
        raise Exception("Environment variable AWS_REGION is not set.")

    # --- Connectivity check over HTTPS ---
    try:
        context = ssl.create_default_context()
        with socket.create_connection((neptune_endpoint, 8182), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=neptune_endpoint):
                print(f"Neptune endpoint {neptune_endpoint}:8182 reachable over HTTPS")
    except Exception as e:
        raise Exception(
            f"Cannot reach Neptune endpoint '{neptune_endpoint}' over HTTPS. "
            f"Check VPC, Security Groups, and subnet routing. Original error: {e}"
        )

    # --- Initialize boto3 Neptune Data client ---
    client = boto3.client("neptunedata", region_name=s3_bucket_region)

    # --- Trigger the bulk loader ---
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
        print("Neptune bulk load triggered successfully")
        return response
    except (BotoCoreError, ClientError) as e:
        raise Exception(f"Failed to start Neptune bulk load. Error: {e}")
