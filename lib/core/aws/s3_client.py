# lib/core/aws/s3_client.py
import boto3
import os
from botocore.exceptions import ClientError
from lib.core.logging import logger
from dotenv import load_dotenv

# Try multiple possible env paths
ENV_PATHS = [
    "env/local.env",                        # local dev
    "/home/ec2-user/neptune_part_match/.env"  # EC2
]

for path in ENV_PATHS:
    if os.path.exists(path):
        load_dotenv(dotenv_path=path)
        logger.info(f"Loaded environment from {path}")
        break
else:
    logger.warning("No .env file found, relying on system environment variables")

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def upload_file(file_path: str, key: str):
    try:
        s3.upload_file(file_path, S3_BUCKET_NAME, key)
        logger.info(f"Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{key}")
    except ClientError as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        raise

def download_file(key: str, dest_path: str):
    try:
        s3.download_file(S3_BUCKET_NAME, key, dest_path)
        logger.info(f"Downloaded s3://{S3_BUCKET_NAME}/{key} to {dest_path}")
    except ClientError as e:
        logger.error(f"Failed to download {key} from S3: {e}")
        raise
