# lib/core/utils/container.py

from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.match_part_usecase import MatchPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

# Import repositories
from lib.app.adapter.output.persistence.neptune.neptune_repository import NeptuneRepository
# In future, you can add:
# from lib.app.adapter.output.persistence.other_db.other_repository import OtherRepository

# ------------------------
# Repository Factory
# ------------------------
def get_repository(db_type: str = "neptune"):
    """
    Returns the repository instance based on db_type.
    Defaults to Neptune.
    """
    if db_type == "neptune":
        return NeptuneRepository()
    # Example for future DBs:
    # elif db_type == "other":
    #     return OtherRepository()
    else:
        raise ValueError(f"Unknown repository type: {db_type}")

# ------------------------
# Use Cases
# ------------------------
def get_part_usecase(db_type: str = "neptune"):
    repo = get_repository(db_type)
    return CrudPartUseCase(repo)

def get_match_usecase(db_type: str = "neptune"):
    repo = get_repository(db_type)
    return MatchPartUseCase(repo)

def get_file_usecase(backup_to_s3: bool = False, db_type: str = "neptune"):
    repo = get_repository(db_type)
    return UploadFileUseCase(repository=repo, backup_to_s3=backup_to_s3)
