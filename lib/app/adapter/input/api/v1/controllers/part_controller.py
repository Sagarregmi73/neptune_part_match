from fastapi import APIRouter, HTTPException, UploadFile, File, Depends
from typing import List
from lib.app.domain.dtos.part_number_dto import PartNumberDTO
from lib.app.domain.entities.part_number import PartNumber
from lib.app.application.use_cases.crud_part_usecase import CrudPartUseCase
from lib.app.application.use_cases.upload_file_usecase import UploadFileUseCase
from lib.core.utils.container import get_part_repository, get_file_usecase

router = APIRouter()

def get_usecase(): 
    return CrudPartUseCase(get_part_repository())

@router.post("/", response_model=PartNumberDTO)
def create_part(part_dto: PartNumberDTO, usecase: CrudPartUseCase = Depends(get_usecase)):
    part = PartNumber(part_dto.part_number, part_dto.specs, part_dto.notes)
    return PartNumberDTO(**vars(usecase.create_part(part)))

@router.get("/{part_number}", response_model=PartNumberDTO)
def get_part(part_number: str, usecase: CrudPartUseCase = Depends(get_usecase)):
    part = usecase.get_part(part_number)
    if not part:
        raise HTTPException(status_code=404, detail="Part not found")
    return PartNumberDTO(**vars(part))

@router.put("/{part_number}", response_model=PartNumberDTO)
def update_part(part_number: str, part_dto: PartNumberDTO, usecase: CrudPartUseCase = Depends(get_usecase)):
    part = PartNumber(part_number, part_dto.specs, part_dto.notes)
    return PartNumberDTO(**vars(usecase.update_part(part)))

@router.delete("/{part_number}")
def delete_part(part_number: str, usecase: CrudPartUseCase = Depends(get_usecase)):
    return {"success": usecase.delete_part(part_number)}

@router.get("/", response_model=List[PartNumberDTO])
def list_parts(usecase: CrudPartUseCase = Depends(get_usecase)):
    return [PartNumberDTO(**vars(p)) for p in usecase.list_parts()]

@router.post("/upload")
async def upload_parts(file: UploadFile = File(...), file_usecase: UploadFileUseCase = Depends(get_file_usecase)):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only XLSX files are supported")
    from io import BytesIO
    file_content = await file.read()
    return file_usecase.execute(BytesIO(file_content), file.filename)
