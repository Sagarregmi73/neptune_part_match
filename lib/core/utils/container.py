# lib/core/utils/container.py

import os
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository

# ---------------- Repository Instances ----------------
neptune_repository = NeptuneRepository()

# ---------------- Repository Factory ----------------
def get_part_repository():
    """
    Returns the repository instance for parts.
    Can be extended later for multiple DB support.
    """
    return neptune_repository

# ---------------- CRUD Usecase Factory ----------------
def get_part_usecase():
    """
    Returns an instance of CrudPartUseCase
    """
    repository = get_part_repository()
    return CrudPartUseCase(repository=repository)

# ---------------- Match Usecase Factory ----------------
def get_match_usecase():
    """
    Returns an instance of MatchPartUseCase
    """
    repository = get_part_repository()
    return MatchPartUseCase(repository)

# ---------------- File Upload Usecase Factory ----------------
def get_file_usecase(backup_to_s3: bool = True):
    """
    Returns an instance of UploadFileUseCase
    """
    return UploadFileUseCase(backup_to_s3=backup_to_s3)
