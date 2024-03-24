from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.models import Paragraph, Author
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import logging

logging.getLogger().addHandler(logging.StreamHandler())

async def load_csv_data(file):
    async with async_session() as db:
        try:
            # Check if the table exists and has data
            conn = await db.connection()
            has_table = await conn.run_sync(
                lambda conn: conn.dialect.has_table(conn, Paragraph.__tablename__)
            )
            
            if has_table:
                # Check if the table has any data
                result = await conn.execute(select(func.count()).select_from(Paragraph))
                count = result.scalar()
                if count > 0:
                    print(f"Dataset is already loaded with {count} entries. Skipping loading process.")
                    return
            
            # Load the dataset if the table doesn't exist or is empty
            df = pd.read_csv(file)
            df['within_page_order'] = df.groupby('page_name').cumcount()
            
            if not has_table:
                # Create the table if it doesn't exist
                await conn.run_sync(Paragraph.__table__.create)
                await conn.commit()  # Commit the table creation transaction
                
            try:
                # Insert the data into the database
                await conn.run_sync(
                    lambda conn: df.to_sql(
                        name=Paragraph.__tablename__,
                        con=conn,
                        if_exists="append",
                        index=False,
                    )
                )
                
                # Commit the data insertion transaction
                await conn.commit()
                
            except Exception as e:
                # Rollback the data insertion transaction if an error occurs
                await conn.rollback()
                raise e
            
        except Exception as e:
            logging.error(f"Error loading CSV data helper: {str(e)}")
            
        finally:
            logging.info("Data loaded successfully")

async def get_random_samples_raw(n: int, db: AsyncSession):
    query = select(Paragraph).order_by(func.random()).limit(n)
    result = await db.execute(query)
    samples = result.scalars().all()
    return samples

async def get_random_samples_raw_as_df(n: int, db: AsyncSession):
    query = select(Paragraph).order_by(func.random()).limit(n)
    result = await db.execute(query)
    samples = result.scalars().all()
    df = pd.DataFrame([sample.__dict__ for sample in samples])
    df = df.drop(columns=['_sa_instance_state'])
    return df

async def get_random_unprocessed_paragraph(db: AsyncSession):
    query = select(Paragraph).filter(Paragraph.processed == -1).order_by(func.random()).limit(1)
    result = await db.execute(query)
    paragraph = result.scalar()
    if paragraph is None:
        return -1
    return paragraph

async def get_page_raw(db: AsyncSession, index: int = -1):
    if index == -1: # get all the paragraphs with the same (randomly selected) page_name
        query = select(Paragraph.page_name).distinct().order_by(func.random()).limit(1)
        result = await db.execute(query)
        page_name = result.scalar()
        query = select(Paragraph).filter(Paragraph.page_name == page_name)
    else: # get paragraphs with pagename in order
        query = select(Paragraph.page_name).distinct().order_by(Paragraph.page_name).offset(index).limit(1)
        result = await db.execute(query)
        page_name = result.scalar()
        query = select(Paragraph).filter(Paragraph.page_name == page_name)
    result = await db.execute(query)
    samples = result.scalars().all()
    return samples

async def create_author_if_not_exists(db: AsyncSession, prompt: str, model: str):
    author = db.query(Author).filter(Author.model == model, Author.prompt == prompt).first()
    if author is None:
        author = Author(model=model, prompt=prompt)
        db.add(author)
        await db.commit()
    return author
        
        


