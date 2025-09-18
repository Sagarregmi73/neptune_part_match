import os
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.app.application.services.file_service import FileService

# -------------------- Repositories --------------------

def get_part_repository():
    """
    Returns the repository for parts.
    Currently supports only Neptune.
    """
    db_type = (os.getenv("DB_TYPE", "NEPTUNE") or "").strip().upper()
    if db_type == "NEPTUNE":
        return NeptuneRepository()
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}. Only NEPTUNE supported currently.")

def get_match_repository():
    """
    Returns the repository for matches.
    For now, it is the same as the part repository.
    """
    return get_part_repository()

# -------------------- File Services & Use Cases --------------------

def get_file_service() -> FileService:
    """
    Returns an instance of FileService.
    """
    return FileService()

def get_file_usecase(backup_to_s3: bool = True) -> UploadFileUseCase:
    """
    Returns an instance of UploadFileUseCase.
    Handles Excel uploads and Neptune bulk load.
    """
    return UploadFileUseCase(backup_to_s3=backup_to_s3)
