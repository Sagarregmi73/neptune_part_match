import os
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.services.file_service import FileService
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

def get_part_repository():
    db_type = (os.getenv("DB_TYPE", "NEPTUNE") or "").strip().upper()
    if db_type == "NEPTUNE":
        return NeptuneRepository()
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}. Only NEPTUNE supported currently")

def get_match_repository():
    return get_part_repository()

def get_file_service():
    return FileService()

def get_file_usecase(backup_to_s3: bool = True):
    """
    Returns an instance of UploadFileUseCase.
    backup_to_s3=True  -> creates CSV backup in S3
    backup_to_s3=False -> only inserts into Neptune
    """
    return UploadFileUseCase(backup_to_s3=backup_to_s3)
