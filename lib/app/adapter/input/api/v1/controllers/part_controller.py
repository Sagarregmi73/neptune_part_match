# lib/app/adapter/input/api/v1/controllers/part_controller.py

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from typing import List
from io import BytesIO

from lib.app.domain.dtos.part_number_dto import PartNumberDTO
from lib.app.domain.entities.part_number import PartNumber
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.core.utils.container import get_part_usecase, get_file_usecase

router = APIRouter()

# Create a part
@router.post("/", response_model=PartNumberDTO)
def create_part(part_dto: PartNumberDTO, usecase: CrudPartUseCase = Depends(get_part_usecase)):
    return usecase.create_part(PartNumber(**part_dto.dict()))

# Get a part by part_number
@router.get("/{part_number}", response_model=PartNumberDTO)
def get_part(part_number: str, usecase: CrudPartUseCase = Depends(get_part_usecase)):
    part = usecase.get_part(part_number)
    if not part:
        raise HTTPException(status_code=404, detail="Part not found")
    return part

# Update a part
@router.put("/{part_number}", response_model=PartNumberDTO)
def update_part(part_number: str, part_dto: PartNumberDTO, usecase: CrudPartUseCase = Depends(get_part_usecase)):
    # Ensure part_number matches
    if part_number != part_dto.part_number:
        raise HTTPException(status_code=400, detail="Part number mismatch")
    return usecase.update_part(PartNumber(**part_dto.dict()))

# Delete a part
@router.delete("/{part_number}")
def delete_part(part_number: str, usecase: CrudPartUseCase = Depends(get_part_usecase)):
    success = usecase.delete_part(part_number)
    return {"success": success}

# List all parts
@router.get("/", response_model=List[PartNumberDTO])
def list_parts(usecase: CrudPartUseCase = Depends(get_part_usecase)):
    return usecase.list_parts()

# Upload Excel file
@router.post("/upload")
async def upload_parts(file: UploadFile = File(...), backup_to_s3: bool = True, file_usecase: UploadFileUseCase = Depends(get_file_usecase)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only XLSX files are supported")
    file_content = await file.read()
    return await file_usecase.execute(BytesIO(file_content), file.filename)
