from fastapi import APIRouter, Depends
from fleecekmbackend.db.helpers import get_random_samples_raw
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from fleecekmbackend.db.models import WikiTextStructured
from fleecekmbackend.db.ctl import get_db

router = APIRouter()

@router.get("/rand-sample")
async def random_samples(n: int, db: AsyncSession = Depends(get_db)):
    samples = await get_random_samples_raw(n, db)
    return samples

async def get_random_samples_raw(n: int, db: AsyncSession):
    query = select(WikiTextStructured).order_by(func.random()).limit(n)
    result = await db.execute(query)
    samples = result.scalars().all()
    return samples


