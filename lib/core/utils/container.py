from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

repository = NeptuneRepository()

def get_part_usecase():
    return CrudPartUseCase(repository)

def get_match_usecase():
    return MatchPartUseCase(repository)

def get_file_usecase(backup_to_s3: bool = True):
    return UploadFileUseCase(get_part_usecase(), get_match_usecase(), backup_to_s3)
