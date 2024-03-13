from fastapi import APIRouter, Depends
from fleecekmbackend.db.utils import get_random_samples
from sqlalchemy.orm import Session
from fleecekmbackend.db.database import get_db

router = APIRouter()


@router.get("/random-samples")
async def random_samples(n: int, db: Session = Depends(get_db)):
    samples = await get_random_samples(n, db)
    return samples
