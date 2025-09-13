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
    iam_role_arn = os.getenv("NEPTUNE_IAM_ROLE_ARN")
    s3_bucket_region = os.getenv("AWS_REGION")  # or the region your S3 bucket is in

    if not (neptune_endpoint and iam_role_arn and s3_bucket_region):
        raise Exception("NEPTUNE_ENDPOINT, NEPTUNE_IAM_ROLE_ARN, and AWS_REGION must be set in environment")

    client = boto3.client("neptunedata", region_name=s3_bucket_region)

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
