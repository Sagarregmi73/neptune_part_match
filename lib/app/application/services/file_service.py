from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase

class FileService:
    def __init__(self, backup_to_s3: bool = True):
        self.upload_usecase = UploadFileUseCase(backup_to_s3=backup_to_s3)

    def execute(self, file_bytes: bytes, filename: str):
        return self.upload_usecase.execute(file_bytes, filename)
