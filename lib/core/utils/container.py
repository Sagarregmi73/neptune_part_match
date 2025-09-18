from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository

neptune_repository = NeptuneRepository()

def get_part_repository():
    return neptune_repository

def get_part_usecase():
    return CrudPartUseCase(neptune_repository)

def get_file_usecase(backup_to_s3: bool = True):
    return UploadFileUseCase(backup_to_s3=backup_to_s3)
