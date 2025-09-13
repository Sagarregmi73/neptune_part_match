import os
import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError

def trigger_bulk_load(s3_input_uri: str, mode: str = "RESUME") -> dict:
    """
    Trigger a Neptune bulk load job safely with connectivity checks.
    
    :param s3_input_uri: The S3 path where vertices/edges CSV files are uploaded.
    :param mode: LOAD | RESUME | NEW (default: RESUME)
    :return: The Neptune bulk loader response
    """
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    s3_bucket_region = os.getenv("AWS_REGION")  # S3 bucket region

    if not neptune_endpoint:
        raise Exception("Environment variable NEPTUNE_ENDPOINT is not set.")
    if not iam_role_arn:
        raise Exception("Environment variable NEPTUNE_IAM_ROLE_ARN is not set.")
    if not s3_bucket_region:
        raise Exception("Environment variable AWS_REGION is not set.")

    # Check if Neptune endpoint is reachable
    status_url = f"http://{neptune_endpoint}:8182/status"
    try:
        response = requests.get(status_url, timeout=5)
        if response.status_code != 200:
            raise Exception(f"Neptune endpoint returned {response.status_code}")
    except requests.exceptions.RequestException as e:
        raise Exception(
            f"Cannot reach Neptune endpoint '{neptune_endpoint}'. "
            f"Check network/VPC settings. Original error: {e}"
        )

    # Initialize boto3 client for Neptune
    client = boto3.client("neptunedata", region_name=s3_bucket_region)

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
