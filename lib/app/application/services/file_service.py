from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

class FileService:
    """
    Service layer for handling Excel uploads.
    Delegates all processing to UploadFileUseCase.
    """

    def __init__(self, backup_to_s3: bool = True):
        self.upload_usecase = UploadFileUseCase(backup_to_s3=backup_to_s3)

    def execute(self, file_bytes: bytes, filename: str):
        """
        Execute file upload: inserts parts & matches into Neptune,
        optionally creates CSV backups in S3.
        """
        return self.upload_usecase.execute(file_bytes, filename)
