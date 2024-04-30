import asyncio
import logging
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from fleecekmbackend.db.models import Paragraph
from fleecekmbackend.core.config import DATABASE_URL, DATASET_PATH
import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create async engine and session
engine = create_async_engine(DATABASE_URL)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def load_csv_data(file_path):
    async with async_session() as db:
        try:
            # Check if the table exists and has data
            async with engine.connect() as conn:
                table_exists = await conn.run_sync(
                    lambda sync_conn: sync_conn.dialect.has_table(sync_conn, Paragraph.__tablename__)
                )
                if table_exists:
                    result = await conn.execute(select(func.max(Paragraph.id)).select_from(Paragraph.__table__))
                    count = result.scalar()
                    if count > 0:
                        logger.info(f"Dataset is already loaded with {count} entries. Skipping loading process.")
                        return

            # Load the dataset if the table doesn't exist or is empty
            df = pd.read_csv(file_path)
            df['within_page_order'] = df.groupby('page_name').cumcount()

            # Replace NaN values with None
            df = df.where(pd.notnull(df), None)

            if not table_exists:
                async with engine.begin() as conn:
                    await conn.run_sync(Paragraph.__table__.create)

            # Insert the data into the database
            async with engine.begin() as conn:
                await conn.execute(text("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'"))

                for _, row in tqdm(df.iterrows(), total=len(df), desc="Inserting data"):
                    await conn.execute(Paragraph.__table__.insert().values(row.to_dict()))

                # Set processed to -1 for all paragraphs
                await conn.execute(Paragraph.__table__.update().values(processed=-1))

        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            # Rollback the transaction if an error occurs
            await conn.rollback()

        finally:
            logger.info("Data loading completed.")

async def main():
    await load_csv_data(DATASET_PATH)

if __name__ == "__main__":
    asyncio.run(main())