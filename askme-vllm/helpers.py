import asyncio
import random
import hashlib

from aiomysql import IntegrityError
from fleecekmbackend.db.ctl import async_session, engine
from fleecekmbackend.db.models import Paragraph, Author, Question, Answer
from sqlalchemy import Column, Integer, func, select, text, update
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
import logging
from tqdm import tqdm


async def load_csv_data_rand_n(file, n, overwrite=False):
    async with async_session() as db:
        try:
            async with engine.connect() as conn:
                table_exists = await conn.run_sync(
                    lambda sync_conn: sync_conn.dialect.has_table(
                        sync_conn, Paragraph.__tablename__
                    )
                )

                if overwrite and table_exists:
                    await conn.execute(
                        text(f"TRUNCATE TABLE {Paragraph.__tablename__}")
                    )
                    logging.info("Existing entries in the database have been removed.")
                elif table_exists and not overwrite:
                    result = await conn.execute(
                        select(func.count()).select_from(Paragraph.__table__)
                    )
                    count = result.scalar()
                    if count and count > 0:
                        logging.info(
                            f"Dataset already contains {count} entries. Use overwrite=True to replace existing data."
                        )
                        return

            df = pd.read_csv(file)
            df["within_page_order"] = df.groupby("page_name").cumcount()
            df = df.where(pd.notnull(df), None)

            # Rename 'id' to 'original_entry_id'
            df = df.rename(columns={"id": "original_entry_id"})

            # Sort by length of 'text' column in reverse order
            df["text_length"] = df["text"].str.len()
            df = df.sort_values("text_length", ascending=False).drop(
                "text_length", axis=1
            )

            # Randomly select N entries
            if len(df) > n:
                df = df.sample(n=n)

            if not table_exists:
                # Modify the Paragraph model to include original_entry_id
                if not hasattr(Paragraph, "original_entry_id"):
                    Paragraph.original_entry_id = Column(Integer)

                async with engine.begin() as conn:
                    await conn.run_sync(Paragraph.__table__.create)
            else:
                # Check if original_entry_id column exists, if not, add it
                async with engine.begin() as conn:
                    result = await conn.execute(
                        text(
                            f"SELECT COUNT(*) FROM information_schema.COLUMNS "
                            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{Paragraph.__tablename__}' "
                            f"AND COLUMN_NAME = 'original_entry_id'"
                        )
                    )
                    column_exists = result.scalar() > 0

                    if not column_exists:
                        await conn.execute(
                            text(
                                f"ALTER TABLE {Paragraph.__tablename__} ADD COLUMN original_entry_id INTEGER"
                            )
                        )

            async with engine.begin() as conn:
                for _, row in tqdm(df.iterrows(), total=len(df), desc="Inserting data"):
                    await conn.execute(
                        Paragraph.__table__.insert().values(row.to_dict())
                    )
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")
            await db.rollback()
            raise  # Re-raise the exception for further debugging if needed
        finally:
            logging.info("Data loading completed.")


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


async def load_csv_data_all(file, overwrite=False):
    async with async_session() as db:
        try:
            async with engine.connect() as conn:
                table_exists = await conn.run_sync(
                    lambda sync_conn: sync_conn.dialect.has_table(
                        sync_conn, Paragraph.__tablename__
                    )
                )

                if overwrite and table_exists:
                    await conn.execute(
                        text(f"TRUNCATE TABLE {Paragraph.__tablename__}")
                    )
                    logging.info("Existing entries in the database have been removed.")
                elif table_exists and not overwrite:
                    result = await conn.execute(
                        select(func.count()).select_from(Paragraph.__table__)
                    )
                    count = result.scalar()
                    if count and count > 0:
                        logging.info(
                            f"Dataset already contains {count} entries. Use overwrite=True to replace existing data."
                        )
                        return

            df = pd.read_csv(file)
            df["within_page_order"] = df.groupby("page_name").cumcount()
            df = df.where(pd.notnull(df), None)

            # Rename 'id' to 'original_entry_id'
            df = df.rename(columns={"id": "original_entry_id"})

            # Sort by length of 'text' column in reverse order
            df["text_length"] = df["text"].str.len()
            df = df.sort_values("text_length", ascending=False).drop(
                "text_length", axis=1
            )

            if not table_exists:
                # Modify the Paragraph model to include original_entry_id
                if not hasattr(Paragraph, "original_entry_id"):
                    Paragraph.original_entry_id = Column(Integer)

                async with engine.begin() as conn:
                    await conn.run_sync(Paragraph.__table__.create)
            else:
                # Check if original_entry_id column exists, if not, add it
                async with engine.begin() as conn:
                    result = await conn.execute(
                        text(
                            f"SELECT COUNT(*) FROM information_schema.COLUMNS "
                            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{Paragraph.__tablename__}' "
                            f"AND COLUMN_NAME = 'original_entry_id'"
                        )
                    )
                    column_exists = result.scalar() > 0

                    if not column_exists:
                        await conn.execute(
                            text(
                                f"ALTER TABLE {Paragraph.__tablename__} ADD COLUMN original_entry_id INTEGER"
                            )
                        )

            # Insert data in chunks for better performance
            chunk_size = 1000  # Adjust this value based on your system's capabilities
            async with engine.begin() as conn:
                for start in tqdm(range(0, len(df), chunk_size), desc="Inserting data"):
                    chunk = df.iloc[start : start + chunk_size]
                    await conn.execute(
                        Paragraph.__table__.insert(),
                        [row.to_dict() for _, row in chunk.iterrows()],
                    )

            logging.info(f"Successfully loaded {len(df)} entries into the database.")

        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")
            await db.rollback()
            raise  # Re-raise the exception for further debugging if needed
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


async def get_unprocessed_paragraphs_count():
    async with async_session() as db:
        try:
            query = select(func.count(Paragraph.id)).where(Paragraph.processed == False)
            result = await db.execute(query)
            count = result.scalar()
            return count
        except Exception as e:
            await db.rollback()
            logging.error(f"Error retrieving unprocessed paragraphs count: {str(e)}")
            return -1


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


async def get_unfiltered_questions_count():
    async with async_session() as db:
        try:
            query = select(func.count(Question.id)).where(Question.filtered == False)
            result = await db.execute(query)
            count = result.scalar()
            return count
        except Exception as e:
            await db.rollback()
            logging.error(f"Error retrieving unfiltered questions count: {str(e)}")
            return -1


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


async def get_unprocessed_questions_count():
    async with async_session() as db:
        try:
            query = select(func.count(Question.id)).where(Question.processed == False)
            result = await db.execute(query)
            count = result.scalar()
            return count
        except Exception as e:
            await db.rollback()
            logging.error(f"Error retrieving unprocessed questions count: {str(e)}")
            return -1


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


async def get_unprocessed_answers_count():
    async with async_session() as db:
        try:
            query = select(func.count(Answer.id)).where(Answer.processed == False)
            result = await db.execute(query)
            count = result.scalar()
            return count
        except Exception as e:
            await db.rollback()
            logging.error(f"Error retrieving unprocessed answers count: {str(e)}")
            return -1


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


def generate_hash(model: str, prompt: str) -> str:
    return hashlib.sha256(f"{model}:{prompt}".encode("utf-8")).hexdigest()


async def create_author_if_not_exists(
    prompt: str, model: str, max_retries: int = 3, initial_delay: float = 1.0
):
    hash_value = generate_hash(model, prompt)

    async def attempt_create_author(db: AsyncSession):
        # Check if the author already exists
        async with db.begin():
            result = await db.execute(
                select(Author).where(Author.hash == hash_value).with_for_update()
            )
            existing_author = result.scalars().first()

            if existing_author:
                return existing_author.id

            # If the author does not exist, insert a new one
            insert_stmt = insert(Author).values(
                model=model, prompt=prompt, hash=hash_value
            )
            try:
                result = await db.execute(insert_stmt)
                await db.commit()
                author_id = result.inserted_primary_key[0]
                return author_id
            except IntegrityError:
                # Handle race condition where another transaction might have inserted the same record
                await db.rollback()
                result = await db.execute(
                    select(Author).where(Author.hash == hash_value)
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
