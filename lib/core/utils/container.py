from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

def get_repository():
    return NeptuneRepository()

def get_part_usecase():
    return CrudPartUseCase(get_repository())

def get_match_usecase():
    return MatchPartUseCase(get_repository())

def get_file_usecase(backup_to_s3: bool = True):
    part_uc = get_part_usecase()
    match_uc = get_match_usecase()
    return UploadFileUseCase(part_uc, match_uc, backup_to_s3)
