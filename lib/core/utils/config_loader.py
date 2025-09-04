import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/local.env")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = int(os.getenv("NEPTUNE_PORT", 8182))

DB_TYPE = os.getenv("DB_TYPE", "NEPTUNE")
NEPTUNE_IAM_ROLE_ARN=os.getenv("NEPTUNE_IAM_ROLE_ARN")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "db": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

MONGO_URI = os.getenv("MONGO_URI")
