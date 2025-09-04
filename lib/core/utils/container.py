# lib/core/utils/container.py
import os
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.adapter.output.persistence.postgres.postgres_repository import PostgresRepository
from lib.app.adapter.output.persistence.mongo.mongo_repository import MongoRepository

DB_TYPE = os.getenv("DB_TYPE", "NEPTUNE").upper()

# Lazy import for FileService to avoid circular import
def get_file_service():
    from lib.app.application.services.file_service import FileService
    return FileService()

def get_part_repository():
    if DB_TYPE == "NEPTUNE":
        return NeptuneRepository()
    elif DB_TYPE == "POSTGRES":
        return PostgresRepository()
    elif DB_TYPE == "MONGO":
        return MongoRepository()
    else:
        raise ValueError(f"Unknown DB_TYPE: {DB_TYPE}")

def get_match_repository():
    return get_part_repository()  # same repo type for matches
