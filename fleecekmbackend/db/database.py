from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from fleecekmbackend.core.config import DATABASE_URL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


async def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def create_tables():
    Base.metadata.create_all(bind=engine)


async def create_tables_if_not_exist():
    async with engine.begin() as conn:
        existing_tables = await conn.run_sync(Base.metadata.reflect)
        if not existing_tables:
            await create_tables()


async def delete_tables():
    Base.metadata.drop_all(bind=engine)
