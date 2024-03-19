from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fleecekmbackend.core.config import DATABASE_URL
from sqlalchemy import func
from fleecekmbackend.db.models import WikiTextStructured
import pandas as pd

engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)
Base = declarative_base()

async def get_db():
    async with async_session() as db:
        yield db

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def create_tables_if_not_exist():
    async with engine.begin() as conn:
        existing_tables = await conn.run_sync(lambda conn: list(Base.metadata.tables.keys()))
        if not existing_tables:
            await create_tables()

async def delete_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

async def get_random_samples_raw(n: int, db):
    query = db.query(WikiTextStructured).order_by(func.random()).limit(n)
    samples = query.all()
    return samples

async def get_random_samples_raw_as_df(n: int, db):
    query = db.query(WikiTextStructured).order_by(func.random()).limit(n)
    samples = query.all()
    df = pd.DataFrame([sample.__dict__ for sample in samples])
    return df