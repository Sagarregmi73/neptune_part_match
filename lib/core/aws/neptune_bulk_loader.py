import os
import boto3

def trigger_bulk_load(s3_input_uri: str, mode: str = "RESUME") -> dict:
    """
    Trigger a Neptune bulk load job.
    :param s3_input_uri: The S3 path where vertices/edges CSV files are uploaded.
    :param mode: LOAD | RESUME | NEW (default: RESUME)
    :return: The Neptune bulk loader response
    """
    neptune_endpoint = os.getenv("NEPTUNE_ENDPOINT")
    neptune_port = os.getenv("NEPTUNE_PORT", "8182")
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")

    if not (neptune_endpoint and iam_role_arn):
        raise Exception("NEPTUNE_ENDPOINT and NEPTUNE_IAM_ROLE_ARN must be set in environment")

    client = boto3.client("neptune-data", region_name=os.getenv("AWS_REGION"))

    response = client.start_loader_job(
        source=s3_input_uri,
        format="csv",
        roleArn=iam_role_arn,
        region=os.getenv("AWS_REGION"),
        failOnError=True,
        parallelism="MEDIUM",
        updateSingleCardinalityProperties=True,
        mode=mode,
    )

    return response
