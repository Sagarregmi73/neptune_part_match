import os
import boto3

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )

def upload_file_to_s3(local_path: str, s3_key: str) -> str:
    bucket = os.getenv("S3_BUCKET_NAME")
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket, s3_key)
    return f"s3://{bucket}/{s3_key}"
