import asyncio
import logging
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple

from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.helpers import (
    get_next_unprocessed_paragraphs,
    get_next_unprocessed_questions,
    get_next_unprocessed_answers,
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
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def generate_questions_stage(db: AsyncSession, paragraph: Paragraph) -> List[int]:
    try:
        question_ids = await generate_questions_single_turn(db, paragraph)
        logging.info(f"Generated questions: {question_ids}")
        return question_ids
    except Exception as e:
        logging.error(f"Error generating questions for paragraph: {paragraph.id}")
        logging.error(str(e))
        raise


async def generate_answers_stage(db: AsyncSession, question_id: int) -> List[int]:
    generated_answer_ids = []
    try:
        for setting in ["zs", "ic"]:
            answer_id = await generate_answer(db, question_id, setting)
            generated_answer_ids.append(answer_id)
            logging.info(f"Generated answer ID: {answer_id}")
        return generated_answer_ids
    except Exception as e:
        logging.error(f"Error generating answers for question: {question_id}")
        logging.error(str(e))
        raise


async def generate_ratings_stage(db: AsyncSession, answer_id: int) -> int:
    try:
        rating_id = await generate_answer_rating(db, answer_id)
        logging.info(f"Generated rating ID: {rating_id}")
        return rating_id
    except Exception as e:
        logging.error(f"Error generating rating for answer: {answer_id}")
        logging.error(str(e))
        raise


async def process_all_paragraphs_s2s(batch_size=5):
    # Stage 1: Generate Questions
    while True:
        async with async_session() as db:
            paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
            if not paragraphs:
                logging.info("No unprocessed paragraphs found. Stopping the process.")
                break
            logging.info(f"Processing {len(paragraphs)} paragraphs")
            tasks = []
            for paragraph in paragraphs:
                task = asyncio.create_task(generate_questions_stage(db, paragraph))
                tasks.append(task)
            await asyncio.gather(*tasks)
            db.flush()
            db.commit()
    logging.info(f"Generated questions for {len(paragraphs)} paragraphs")

    # Stage 2: Generate Answers
    while True:
        async with async_session() as db:
            questions = await get_next_unprocessed_questions(db, batch_size)
            if not questions:
                logging.info("No unprocessed questions found. Stopping the process.")
                break
            logging.info(f"Processing {len(questions)} questions")
            tasks = []
            for question in questions:
                task = asyncio.create_task(generate_answers_stage(db, question.id))
                tasks.append(task)
            await asyncio.gather(*tasks)
            db.flush()
            db.commit()
    logging.info(f"Generated answers for {len(questions)} questions")

    # Stage 3: Generate Ratings
    while True:
        async with async_session() as db:
            answers = await get_next_unprocessed_answers(db, batch_size)
            if not answers:
                logging.info("No unprocessed answers found. Stopping the process.")
                break
            logging.info(f"Processing {len(answers)} answers")
            tasks = []
            for answer in answers:
                task = asyncio.create_task(generate_ratings_stage(db, answer.id))
                tasks.append(task)
            await asyncio.gather(*tasks)
            db.flush()
            db.commit()
    logging.info(f"Generated ratings for {len(answers)} answers")


async def start_background_process_s2s(batch_size=64):
    try:
        await process_all_paragraphs_s2s(batch_size)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))
