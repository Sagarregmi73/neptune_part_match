# lib/app/application/use_cases/upload_file_usecase.py
from lib.app.application.services.file_service import FileService

class UploadFileUseCase:
    """
    Use case for handling Excel file uploads and bulk loading into Neptune.
    """

    def __init__(self, file_service: FileService):
        self.file_service = file_service

    def execute(self, file_obj, filename: str):
        """
        Process the uploaded Excel file:
        1. Save locally
        2. Generate vertices & edges CSVs
        3. Upload CSVs to S3
        4. Trigger Neptune bulk loader
        """
        result = self.file_service.process_file(file_obj, filename)
        return result
