# lib/app/application/use_cases/upload_file_usecase.py
from lib.app.application.services.file_service import FileService

class UploadFileUseCase:
    def __init__(self, file_service: FileService):
        self.file_service = file_service

    def execute(self, file_obj, filename: str):
        """
        Process the file end-to-end:
        - Save locally
        - Parse to nodes & edges CSV
        - Upload to S3
        - Trigger Neptune bulk load
        """
        result = self.file_service.process_file(file_obj, filename)
        return result
