import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.helpers import (
    get_next_unprocessed_paragraphs,
)
from fleecekmbackend.db.models import (
    Paragraph,
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


def generate_questions_stage(db: AsyncSession, paragraph: Paragraph) -> List[int]:
    try:
        question_ids = generate_questions_single_turn(db, paragraph)
        logging.info(f"Generated questions: {question_ids}")
        return question_ids
    except Exception as e:
        logging.error(f"Error generating questions for paragraph: {paragraph.id}")
        logging.error(str(e))
        raise


def generate_answers_stage(db: AsyncSession, question_id: int) -> List[int]:
    generated_answer_ids = []
    try:
        for setting in ["zs", "ic"]:
            answer_id = generate_answer(db, question_id, setting)
            generated_answer_ids.append(answer_id)
            logging.info(f"Generated answer ID: {answer_id}")
        return generated_answer_ids
    except Exception as e:
        logging.error(f"Error generating answers for question: {question_id}")
        logging.error(str(e))
        raise


def generate_ratings_stage(db: AsyncSession, question_id: int, answer_id: int) -> int:
    try:
        rating_id = generate_answer_rating(db, question_id, answer_id)
        logging.info(f"Generated rating ID: {rating_id}")
        return rating_id
    except Exception as e:
        logging.error(f"Error generating rating for answer: {answer_id}")
        logging.error(str(e))
        raise


async def process_all_paragraphs_s2s(batch_size=5):
    with async_session() as db:
        while True:
            paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
            if not paragraphs:
                logging.info("No unprocessed paragraphs found. Stopping the process.")
                break

            # Stage 1: Generate Questions
            logging.info(f"Processing {len(paragraphs)} paragraphs")
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_paragraph = {
                    executor.submit(generate_questions_stage, db, paragraph): paragraph
                    for paragraph in paragraphs
                }
                questions_by_paragraph = []
                for future in as_completed(future_to_paragraph):
                    try:
                        questions_by_paragraph.append(future.result())
                    except Exception as e:
                        logging.error(f"Error in generate_questions_stage: {e}")

            # Flatten the list of lists of question IDs
            question_ids = [qid for qlist in questions_by_paragraph for qid in qlist]

            # Stage 2: Generate Answers
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_question = {
                    executor.submit(
                        generate_answers_stage, db, question_id
                    ): question_id
                    for question_id in question_ids
                }
                answers_by_question = []
                for future in as_completed(future_to_question):
                    try:
                        answers_by_question.append(future.result())
                    except Exception as e:
                        logging.error(f"Error in generate_answers_stage: {e}")

            # Flatten the list of lists of answer IDs
            answer_ids = [aid for alist in answers_by_question for aid in alist]

            # Stage 3: Generate Ratings
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                future_to_answer = {
                    executor.submit(
                        generate_ratings_stage, db, question_id, answer_id
                    ): (question_id, answer_id)
                    for question_id, answer_id in zip(question_ids, answer_ids)
                }
                for future in as_completed(future_to_answer):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error in generate_ratings_stage: {e}")

            # Mark paragraphs as processed
            for paragraph in paragraphs:
                db.execute(
                    update(Paragraph)
                    .where(Paragraph.id == paragraph.id)
                    .values(processed=True)
                )
            db.commit()


async def start_background_process_s2s(batch_size=64):
    try:
        await process_all_paragraphs_s2s(batch_size)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))
