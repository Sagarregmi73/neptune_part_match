import boto3
import os

def get_s3_client():
    region = os.getenv("AWS_REGION")
    print(f"DEBUG: AWS_REGION='{region}'")  # Debug logging
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region,
    )

def upload_file_to_s3(local_file_path: str, s3_key: str) -> str:
    """
    Upload a file to the configured S3 bucket.
    Returns the full s3:// path.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise Exception("S3_BUCKET_NAME not set in environment")

    s3 = get_s3_client()
    s3.upload_file(local_file_path, bucket_name, s3_key)

    return f"s3://{bucket_name}/{s3_key}"
