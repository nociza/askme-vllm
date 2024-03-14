from fastapi import APIRouter, UploadFile, File
from fleecekmbackend.services.dataset.creation import load_csv_data

router = APIRouter()


@router.post("/load-csv")
async def load_csv(file: UploadFile = File(...)):
    await load_csv_data(file)
    return {"message": "CSV data loaded successfully"}
