# lib/core/config.py
import os
from dotenv import load_dotenv
from lib.core.logging import logger

def load_environment():
    """
    Dynamically load environment variables:
    - Local: env/local.env
    - EC2/Docker: .env
    - Else: system environment variables
    """
    tried_paths = ["env/local.env", ".env"]

    for path in tried_paths:
        if os.path.exists(path):
            load_dotenv(dotenv_path=path)
            logger.info(f"✅ Loaded environment from {path}")
            return

    logger.warning("⚠️ No .env file found, relying on system environment variables only")


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
