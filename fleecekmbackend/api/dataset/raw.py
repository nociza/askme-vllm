from fastapi import APIRouter, Depends
from fleecekmbackend.db.helpers import get_random_samples_raw
from sqlalchemy.orm import Session
from fleecekmbackend.db.ctl import get_db

router = APIRouter()


@router.get("/rand-sample")
async def random_samples(n: int, db: Session = Depends(get_db)):
    samples = await get_random_samples_raw(n, db)
    return samples


