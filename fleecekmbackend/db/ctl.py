from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
from fleecekmbackend.core.config import DATABASE_URL

engine = create_async_engine(
    DATABASE_URL, pool_size=4096, max_overflow=4096, pool_recycle=3600
)
async_session = sessionmaker(engine, class_=AsyncSession, autoflush=True)
Base = declarative_base()


async def get_db():
    async with async_session() as db:
        yield db


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def create_tables_if_not_exist():
    async with engine.begin() as conn:
        await create_tables()


async def delete_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
