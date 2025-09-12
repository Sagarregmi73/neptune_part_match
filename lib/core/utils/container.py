import os
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.services.file_service import FileService

DB_TYPE = os.getenv("DB_TYPE", "NEPTUNE").upper()

def get_part_repository():
    if DB_TYPE == "NEPTUNE":
        return NeptuneRepository()
    else:
        raise ValueError("Only NEPTUNE DB_TYPE supported currently")

def get_match_repository():
    return get_part_repository()

def get_file_service():
    return FileService()
