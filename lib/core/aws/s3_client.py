import boto3
import os

def get_s3_client():
    region = os.getenv("AWS_REGION")
    print(f"DEBUG: AWS_REGION='{region}'")  # 👈 will show hidden spaces
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region,
    )
