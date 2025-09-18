import os
import socket
import ssl
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def trigger_bulk_load(s3_input_uri: str, mode: str = "NEW") -> dict:
    """
    Trigger a Neptune bulk load job from S3 folder.
    """
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    aws_region = os.getenv("AWS_REGION")

    if not all([neptune_endpoint, iam_role_arn, aws_region]):
        raise Exception("NEPTUNE_ENDPOINT, NEPTUNE_IAM_ROLE_ARN, AWS_REGION must be set in env")

    # Test connectivity
    try:
        socket.gethostbyname(neptune_endpoint)
        context = ssl.create_default_context()
        with socket.create_connection((neptune_endpoint, 8182), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=neptune_endpoint):
                pass
    except Exception as e:
        raise Exception(f"Cannot reach Neptune endpoint '{neptune_endpoint}:8182'. {e}")

    client = boto3.client(
        "neptunedata",
        region_name=aws_region,
        endpoint_url=f"https://{neptune_endpoint}:8182"
    )

    try:
        response = client.start_loader_job(
            source=s3_input_uri,
            format="csv",
            iamRoleArn=iam_role_arn,
            s3BucketRegion=aws_region,
            mode=mode,
            failOnError=True,
            parallelism="MEDIUM",
            updateSingleCardinalityProperties=True
        )
        return {"loadId": response.get("loadId"), "status": response.get("status")}
    except (BotoCoreError, ClientError) as e:
        raise Exception(f"Failed to start Neptune bulk load. Error: {e}")
