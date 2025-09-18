# lib/core/aws/s3_client.py

import boto3
import os

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

def upload_file_to_s3(local_path: str, s3_key: str):
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME not set in environment")

    s3_client.upload_file(local_path, bucket_name, s3_key)
    print(f"Uploaded {s3_key} to bucket {bucket_name}")
