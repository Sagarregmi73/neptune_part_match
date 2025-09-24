from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository

repository = NeptuneRepository()

def get_part_repository(): return repository
def get_part_usecase(): return CrudPartUseCase(get_part_repository())
def get_match_usecase(): return MatchPartUseCase(get_part_repository())
def get_file_usecase(backup_to_s3: bool = True): return UploadFileUseCase(get_part_repository(), backup_to_s3)
