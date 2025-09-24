class FileService:
    """
    Generic service for file-related operations.
    """
    def process_file(self, file_bytes, filename: str):
        return {"filename": filename, "status": "processed"}
