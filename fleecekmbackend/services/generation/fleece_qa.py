import logging
import asyncio
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple
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


async def process_paragraph(
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
                    rating_id = await generate_answer_rating(db, question_id, answer_id)
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


async def process_paragraph_with_retry(
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
                        rating_id = await generate_answer_rating(
                            db, question_id, answer_id
                        )
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
