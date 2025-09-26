# lib/core/utils/container.py

from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

# Initialize repository (singleton)
repository = NeptuneRepository()

# Dependency provider for Parts CRUD
def get_part_usecase():
    return CrudPartUseCase(repository)

# Dependency provider for Matches CRUD
def get_match_usecase():
    return MatchPartUseCase(repository)

# Dependency provider for Upload Use Case
def get_file_usecase(backup_to_s3: bool = True):
    return UploadFileUseCase(
        part_usecase=get_part_usecase(),
        match_usecase=get_match_usecase(),
        backup_to_s3=backup_to_s3
    )
