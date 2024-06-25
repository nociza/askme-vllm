import asyncio
import random

from aiomysql import IntegrityError
from fleecekmbackend.db.ctl import async_session, engine
from fleecekmbackend.db.models import Paragraph, Author, Question, Answer
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import logging
from tqdm import tqdm


async def load_csv_data_top_n(file, n):
    async with async_session() as db:
        try:
            async with engine.connect() as conn:
                table_exists = await conn.run_sync(
                    lambda sync_conn: sync_conn.dialect.has_table(
                        sync_conn, Paragraph.__tablename__
                    )
                )
                if table_exists:
                    result = await conn.execute(
                        select(func.max(Paragraph.id)).select_from(Paragraph.__table__)
                    )
                    count = result.scalar()
                    if count and count > 0:
                        logging.info(
                            f"Dataset is already loaded with {count} entries. Skipping loading process."
                        )
                        return
            df = pd.read_csv(file)
            df["within_page_order"] = df.groupby("page_name").cumcount()
            df = df.where(pd.notnull(df), None)
            df = df.head(n)

            if not table_exists:
                async with engine.begin() as conn:
                    await conn.run_sync(Paragraph.__table__.create)
            async with engine.begin() as conn:
                await conn.execute(text("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'"))

                for _, row in tqdm(df.iterrows(), total=n, desc="Inserting data"):
                    await conn.execute(
                        Paragraph.__table__.insert().values(row.to_dict())
                    )
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")
            await conn.rollback()
        finally:
            logging.info("Data loading completed.")


async def load_csv_data(file):
    async with async_session() as db:
        try:
            async with engine.connect() as conn:
                table_exists = await conn.run_sync(
                    lambda sync_conn: sync_conn.dialect.has_table(
                        sync_conn, Paragraph.__tablename__
                    )
                )
                if table_exists:
                    result = await conn.execute(
                        select(func.max(Paragraph.id)).select_from(Paragraph.__table__)
                    )
                    count = result.scalar()
                    if count and count > 0:
                        logging.info(
                            f"Dataset is already loaded with {count} entries. Skipping loading process."
                        )
                        return
            df = pd.read_csv(file)
            df["within_page_order"] = df.groupby("page_name").cumcount()
            df = df.where(pd.notnull(df), None)

            if not table_exists:
                async with engine.begin() as conn:
                    await conn.run_sync(Paragraph.__table__.create)
            async with engine.begin() as conn:
                await conn.execute(text("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'"))

                for _, row in tqdm(df.iterrows(), total=len(df), desc="Inserting data"):
                    await conn.execute(
                        Paragraph.__table__.insert().values(row.to_dict())
                    )
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")
            await conn.rollback()
        finally:
            logging.info("Data loading completed.")


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
    df = df.drop(columns=["_sa_instance_state"])
    return df


async def get_random_unprocessed_paragraphs(db: AsyncSession, n: int = 1):
    try:
        paragraphs = []
        while not paragraphs:
            max_processed = (
                await db.execute(
                    select(func.count(Paragraph.id)).where(Paragraph.processed != -1)
                )
            ).scalar()
            total_paragraphs = (
                await db.execute(select(func.max(Paragraph.id)))
            ).scalar()
            offset = random.randint(0, int(total_paragraphs) - int(max_processed))
            paragraphs = (
                await db.execute(
                    select(Paragraph)
                    .where(Paragraph.processed == -1)
                    .offset(offset)
                    .limit(n)
                )
            ).scalar()
        if not paragraphs:
            raise Exception("No unprocessed paragraphs found")
        elif isinstance(paragraphs, Paragraph):
            return [paragraphs]
        return paragraphs
    except Exception as e:
        logging.error(f"Error retrieving random unprocessed paragraph: {str(e)}")
        return -1


async def get_next_unprocessed_paragraphs(db: AsyncSession, n: int = 1):
    try:
        query = (
            select(Paragraph)
            .filter(Paragraph.processed == False)
            .limit(n)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(query)
        paragraphs = result.scalars().all()
        if not paragraphs:
            raise Exception("No unprocessed paragraphs found")
        return paragraphs

    except Exception as e:
        await db.rollback()
        logging.error(f"Error retrieving next unprocessed paragraphs: {str(e)}")
        return []


async def get_next_unfiltered_questions(db: AsyncSession, n: int = 1):
    try:
        query = (
            select(Question)
            .filter(Question.filtered == False)
            .limit(n)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(query)
        questions = result.scalars().all()
        if not questions:
            raise Exception("No unfiltered questions found")
        return questions

    except Exception as e:
        await db.rollback()
        logging.error(f"Error retrieving next unfiltered questions: {str(e)}")
        return []


async def get_next_unprocessed_questions(db: AsyncSession, n: int = 1):
    try:
        query = (
            select(Question)
            .filter(Question.processed == False)
            .limit(n)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(query)
        questions = result.scalars().all()
        if not questions:
            raise Exception("No unprocessed questions found")
        return questions

    except Exception as e:
        await db.rollback()
        logging.error(f"Error retrieving next unprocessed questions: {str(e)}")
        return []


async def get_next_unprocessed_answers(db: AsyncSession, n: int = 1):
    try:
        query = (
            select(Answer)
            .filter(Answer.processed == False)
            .limit(n)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(query)
        answers = result.scalars().all()
        if not answers:
            raise Exception("No unprocessed answers found")
        return answers

    except Exception as e:
        await db.rollback()
        logging.error(f"Error retrieving next unprocessed answers: {str(e)}")
        return []


async def get_page_raw(db: AsyncSession, index: int = -1):
    if (
        index == -1
    ):  # get all the paragraphs with the same (randomly selected) page_name
        query = select(Paragraph.page_name).distinct().order_by(func.random()).limit(1)
        result = await db.execute(query)
        page_name = result.scalar()
        query = select(Paragraph).filter(Paragraph.page_name == page_name)
    else:  # get paragraphs with pagename in order
        query = (
            select(Paragraph.page_name)
            .distinct()
            .order_by(Paragraph.page_name)
            .offset(index)
            .limit(1)
        )
        result = await db.execute(query)
        page_name = result.scalar()
        query = select(Paragraph).filter(Paragraph.page_name == page_name)
    result = await db.execute(query)
    samples = result.scalars().all()
    return samples


async def create_author_if_not_exists(
    prompt: str, model: str, max_retries: int = 3, initial_delay: float = 1.0
):
    async def attempt_create_author(db: AsyncSession):
        # Check if the author already exists
        async with db.begin():
            result = await db.execute(
                select(Author)
                .where(Author.model == model, Author.prompt == prompt)
                .with_for_update()
            )
            existing_author = result.scalars().first()

            if existing_author:
                return existing_author.id

            # If the author does not exist, insert a new one
            insert_stmt = insert(Author).values(model=model, prompt=prompt)
            try:
                result = await db.execute(insert_stmt)
                await db.commit()
                author_id = result.inserted_primary_key[0]
                return author_id
            except IntegrityError:
                # Handle race condition where another transaction might have inserted the same record
                await db.rollback()
                result = await db.execute(
                    select(Author).where(Author.model == model, Author.prompt == prompt)
                )
                existing_author = result.scalars().first()
                if existing_author:
                    return existing_author.id
                else:
                    raise

    delay = initial_delay
    for attempt in range(max_retries):
        try:
            async with async_session() as db:
                return await attempt_create_author(db)
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed to create author after {max_retries} attempts.")
                raise
            logging.warning(
                f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds..."
            )
            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff

    raise Exception("Unexpected error: All retries failed but no exception was raised.")
