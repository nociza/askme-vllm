import asyncio
import logging
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple

from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.helpers import (
    get_next_unprocessed_paragraphs,
)
from fleecekmbackend.db.models import (
    Paragraph,
    Question,
    Answer,
    Rating,
)
from fleecekmbackend.services.dataset.questions import (
    generate_questions,
    generate_questions_single_turn,
)
from fleecekmbackend.services.dataset.answers import generate_answer
from fleecekmbackend.services.dataset.ratings import generate_answer_rating

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def process_paragraph_e2e(
    db: AsyncSession, paragraph: Paragraph
) -> Tuple[List[Question], List[Answer], List[Rating]]:
    generated_question_ids = []
    generated_answer_ids = []
    generated_rating_ids = []
    try:
        paragraph_id = paragraph.id
        logging.info(f"Processing paragraph: {paragraph_id}")

        question_ids = await generate_questions(db, paragraph)

        logging.info(f"generated_questions: {question_ids}")

        generated_question_ids.extend(question_ids)
        for question_id in question_ids:
            try:
                for setting in ["zs", "ic"]:
                    # Generate answers
                    answer_id = await generate_answer(db, question_id, setting)
                    generated_answer_ids.append(answer_id)
                    logging.info(f"generated_answer_id: {answer_id}")

                    # Generate answer ratings
                    rating_id = await generate_answer_rating(db, answer_id)
                    generated_rating_ids.append(rating_id)
                    logging.info(f"generated_rating_id: {rating_id}")

            except Exception as e:
                logging.error(f"Error processing question: {question_id}")
                logging.error(str(e))
                raise
        await db.execute(
            update(Paragraph).where(Paragraph.id == paragraph_id).values(processed=1)
        )
        await db.commit()
        logging.info(f"Processed paragraph: {paragraph_id}")

    except Exception as e:
        await db.rollback()
        logging.error(str(e))
        raise
    return generated_question_ids, generated_answer_ids, generated_rating_ids


async def process_paragraph_e2e_with_retry(
    db: AsyncSession, paragraph: Paragraph
) -> Tuple[List[Question], List[Answer], List[Rating]]:
    max_retries = 3
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            generated_question_ids = []
            generated_answer_ids = []
            generated_rating_ids = []

            paragraph_id = paragraph.id
            logging.info(f"Processing paragraph: {paragraph_id}")

            question_ids = await generate_questions_single_turn(db, paragraph)
            logging.info(f"generated_questions: {question_ids}")
            generated_question_ids.extend(question_ids)

            for question_id in question_ids:
                try:
                    for setting in ["zs", "ic"]:
                        # Generate answers
                        answer_id = await generate_answer(db, question_id, setting)
                        generated_answer_ids.append(answer_id)
                        logging.info(f"generated_answer_id: {answer_id}")

                        # Generate answer ratings
                        rating_id = await generate_answer_rating(db, answer_id)
                        generated_rating_ids.append(rating_id)
                        logging.info(f"generated_rating_id: {rating_id}")
                except Exception as e:
                    logging.error(f"Error processing question: {question_id}")
                    logging.error(str(e))
                    raise

            await db.execute(
                update(Paragraph)
                .where(Paragraph.id == paragraph_id)
                .values(processed=True)
            )
            await db.commit()
            logging.info(f"Processed paragraph: {paragraph_id}")

            return generated_question_ids, generated_answer_ids, generated_rating_ids

        except Exception as e:
            await db.rollback()
            logging.error(str(e))
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logging.error(
                    f"Max retries reached for paragraph: {paragraph_id}. Skipping."
                )
                raise


async def process_all_pages_e2e():
    while True:
        async with async_session() as db:
            # Check if there are any unprocessed paragraphs
            unprocessed_paragraph_exists = await db.scalar(
                select(func.max(Paragraph.id)).where(Paragraph.processed == -1)
            )

            if not unprocessed_paragraph_exists:
                logging.info(
                    "All paragraphs have been processed. Stopping the process."
                )
                break

            try:
                current_paragraph = await get_next_unprocessed_paragraphs(db)
                print(f"current_paragraph: {current_paragraph}")

                while current_paragraph != -1:
                    try:
                        logging.info(
                            f"Processing section {current_paragraph.section_hierarchy}..."
                        )
                        generated_questions, generated_answers, generated_ratings = (
                            await process_paragraph_e2e(db, current_paragraph)
                        )
                        logging.info(f"generated_questions: {generated_questions}")
                        logging.info(f"generated_answers: {generated_answers}")
                        logging.info(f"generated_ratings: {generated_ratings}")
                    except Exception as e:
                        logging.error(
                            f"Error processing section {current_paragraph.section_hierarchy}"
                        )
                        logging.error(str(e))
                    finally:
                        current_paragraph = await get_next_unprocessed_paragraphs(db)

                logging.info("All pages processed successfully.")
            except Exception as e:
                logging.error(f"Error occurred in process_all_pages: {str(e)}")
                # Wait for a short duration before retrying
                await asyncio.sleep(5)


async def process_all_pages_e2e_parallel(batch_size=5):
    while True:
        async with async_session() as db:
            unprocessed_paragraph_exists = await db.scalar(
                select(func.max(Paragraph.id)).where(Paragraph.processed == False)
            )

            if not unprocessed_paragraph_exists:
                logging.info(
                    "All paragraphs have been processed. Stopping the process."
                )
                break

            try:
                paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
                if not paragraphs:
                    logging.info(
                        "No unprocessed paragraphs found. Stopping the process."
                    )
                    break

                tasks = []
                for paragraph in paragraphs:
                    task = asyncio.create_task(
                        process_paragraph_e2e_with_retry(db, paragraph)
                    )
                    tasks.append(task)

                await asyncio.gather(*tasks)

            except Exception as e:
                logging.error(f"Error occurred in process_all_pages_parallel: {str(e)}")

            await asyncio.sleep(5)


async def start_background_process_e2e():
    try:
        await process_all_pages_e2e_parallel(1)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))
