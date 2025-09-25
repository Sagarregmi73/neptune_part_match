from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from typing import List
from lib.app.domain.dtos.part_number_dto import PartNumberDTO
from lib.app.domain.entities.part_number import PartNumber
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.core.utils.container import get_part_usecase, get_file_usecase
from io import BytesIO

router = APIRouter()

@router.post("/", response_model=PartNumberDTO)
async def create_part(
    part_dto: PartNumberDTO,
    usecase: CrudPartUseCase = Depends(get_part_usecase)
):
    return await usecase.create_part(PartNumber(**part_dto.dict()))

@router.get("/{part_number}", response_model=PartNumberDTO)
async def get_part(
    part_number: str,
    usecase: CrudPartUseCase = Depends(get_part_usecase)
):
    part = await usecase.get_part(part_number)
    if not part:
        raise HTTPException(status_code=404, detail="Part not found")
    return part

@router.put("/{part_number}", response_model=PartNumberDTO)
async def update_part(
    part_number: str,
    part_dto: PartNumberDTO,
    usecase: CrudPartUseCase = Depends(get_part_usecase)
):
    return await usecase.update_part(PartNumber(**part_dto.dict()))

@router.delete("/{part_number}")
async def delete_part(
    part_number: str,
    usecase: CrudPartUseCase = Depends(get_part_usecase)
):
    success = await usecase.delete_part(part_number)
    return {"success": success}

@router.get("/", response_model=List[PartNumberDTO])
async def list_parts(usecase: CrudPartUseCase = Depends(get_part_usecase)):
    return await usecase.list_parts()

@router.post("/upload")
async def upload_parts(
    file: UploadFile = File(...),
    backup_to_s3: bool = True,
    file_usecase: UploadFileUseCase = Depends(get_file_usecase)
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only XLSX files are supported")
    file_content = await file.read()
    return await file_usecase.execute(BytesIO(file_content), file.filename)
