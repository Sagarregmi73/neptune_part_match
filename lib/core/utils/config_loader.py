# lib/core/config.py
import os
from dotenv import load_dotenv
from lib.core.logging import logger

def load_environment():
    """
    Dynamically load environment variables:
    - Local: .env inside container (copied from env/local.env)
    - EC2/Docker: rely on system environment variables injected via `docker run -e`
    """
    local_env = ".env"
    if os.path.exists(local_env):
        load_dotenv(local_env)
        logger.info(f"✅ Loaded environment from {local_env}")
    else:
        logger.info("⚠️ No .env file found, relying on system environment variables")

# Load environment at import
load_environment()

# --- AWS / S3 ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# --- Neptune ---
NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = int(os.getenv("NEPTUNE_PORT", 8182))
NEPTUNE_IAM_ROLE_ARN = os.getenv("NEPTUNE_IAM_ROLE_ARN")
DB_TYPE = os.getenv("DB_TYPE", "NEPTUNE")

# --- Postgres ---
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "db": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# --- Mongo ---
MONGO_URI = os.getenv("MONGO_URI")
