# lib/core/utils/container.py

import os
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository

# ---------------- Repository Instances ----------------
# For now, using NeptuneRepository. Can extend for multiple DBs later.
neptune_repository = NeptuneRepository()

# ---------------- CRUD Usecase Factory ----------------
def get_part_repository():
    """
    Returns the repository instance for parts.
    Can be extended later for multiple DB support.
    """
    return neptune_repository

def get_part_usecase():
    """
    Returns an instance of CrudPartUseCase with the required repository.
    """
    repository = get_part_repository()  # get repository instance
    return CrudPartUseCase(repository)  # pass repository to use case

# ---------------- Match Usecase Factory ----------------
def get_match_usecase():
    """
    Returns the Match usecase instance
    """
    return MatchPartUseCase(get_part_repository())

# ---------------- File Upload Usecase Factory ----------------
def get_file_usecase(backup_to_s3: bool = True):
    """
    Returns an instance of UploadFileUseCase.
    """
    return UploadFileUseCase(backup_to_s3=backup_to_s3)
