# lib/core/aws/s3_client.py
import boto3
import os

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client("s3", region_name=AWS_REGION)

def upload_file(local_path: str, key: str) -> str:
    """
    Upload a file to S3 and return full s3://bucket/key path.
    """
    s3.upload_file(local_path, S3_BUCKET_NAME, key)
    return f"s3://{S3_BUCKET_NAME}/{key}"

def download_file(key: str, local_path: str) -> str:
    """
    Download a file from S3 and return local path.
    """
    s3.download_file(S3_BUCKET_NAME, key, local_path)
    return local_path
