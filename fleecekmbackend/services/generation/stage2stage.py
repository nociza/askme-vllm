import asyncio
import logging
import time
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from collections import deque
from contextlib import asynccontextmanager

from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.helpers import (
    get_next_unfiltered_questions,
    get_next_unprocessed_paragraphs,
    get_next_unprocessed_questions,
    get_next_unprocessed_answers,
)
from fleecekmbackend.db.models import Paragraph, Question, Answer, Rating
from fleecekmbackend.services.dataset.questions import (
    filter_questions,
    generate_questions_single_turn,
)
from fleecekmbackend.services.dataset.answers import generate_answer
from fleecekmbackend.services.dataset.ratings import generate_answer_rating
from fleecekmbackend.core.config import LOGGING_LEVEL

logging.basicConfig(
    level=LOGGING_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def generate_questions_stage(paragraph: Paragraph) -> List[Question]:
    try:
        questions = await generate_questions_single_turn(paragraph)
        return questions
    except Exception as e:
        logging.error(f"Error generating questions for paragraph: {paragraph.id}")
        logging.error(str(e))
        raise


async def filter_questions_stage(questions: List[Question]) -> List[Question]:
    async with async_session() as db:
        try:
            updated_questions = await filter_questions(db, questions)
            return updated_questions
        except Exception as e:
            logging.error(f"Error filtering questions: {questions}")
            logging.error(str(e))
            raise


async def generate_answers_stage(question_id: int) -> List[Answer]:
    generated_answers = []
    async with async_session() as db:
        try:
            for setting in ["zs", "ic"]:
                answer = await generate_answer(db, question_id, setting, flush=False)
                generated_answers.append(answer)
            return generated_answers
        except Exception as e:
            logging.error(f"Error generating answers for question: {question_id}")
            logging.error(str(e))
            raise


async def generate_ratings_stage(answer_id: int) -> Rating:
    async with async_session() as db:
        try:
            rating = await generate_answer_rating(db, answer_id, flush=False)
            return rating
        except Exception as e:
            logging.error(f"Error generating rating for answer: {answer_id}")
            logging.error(str(e))
            raise


async def process_all_paragraphs_s2s(batch_size=5):
    # Stage 1: Generate Questions
    logging.info("Starting stage 1: Generate Questions")
    stage_1_start_time = time.time()
    while True:
        async with async_session() as db:
            paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
            if not paragraphs:
                logging.info("No unprocessed paragraphs found. Stopping the process.")
                break
            logging.info(f"Processing {len(paragraphs)} paragraphs")
            all_questions = await asyncio.gather(
                *[generate_questions_stage(paragraph) for paragraph in paragraphs]
            )
            if all_questions:
                all_questions = [q for questions in all_questions for q in questions]
                db.add_all(all_questions)
                await db.flush()
                await db.commit()
        logging.info(f"Processed {len(paragraphs)} paragraphs")
    stage_1_end_time = time.time()

    # Stage 2: Filter Questions
    logging.info("Starting stage 2: Filter Questions")
    stage_2_start_time = time.time()
    while True:
        async with async_session() as db:
            questions = await get_next_unfiltered_questions(db, batch_size)
            if not questions:
                logging.info("No unprocessed questions found. Stopping the process.")
                break
            logging.info(f"Processing {len(questions)} questions")
            filtered_questions = await filter_questions_stage(questions)
            db.add_all(filtered_questions)
            await db.flush()
            await db.commit()
        logging.info(f"Processed {len(questions)} questions")
    stage_2_end_time = time.time()

    # Stage 3: Generate Answers
    logging.info("Starting stage 3: Generate Answers")
    stage_3_start_time = time.time()
    while True:
        async with async_session() as db:
            questions = await get_next_unprocessed_questions(db, batch_size)
            if not questions:
                logging.info("No unprocessed questions found. Stopping the process.")
                break
            logging.info(f"Processing {len(questions)} questions")
            all_answers = await asyncio.gather(
                *[generate_answers_stage(question.id) for question in questions]
            )
            all_answers = [a for answers in all_answers for a in answers]
            db.add_all(all_answers)
            await db.flush()
            await db.commit()
        logging.info(f"Processed {len(questions)} questions")
    stage_3_end_time = time.time()

    # Stage 4: Generate Ratings
    logging.info("Starting stage 4: Generate Ratings")
    stage_4_start_time = time.time()
    while True:
        async with async_session() as db:
            answers = await get_next_unprocessed_answers(db, batch_size)
            if not answers:
                logging.info("No unprocessed answers found. Stopping the process.")
                break
            logging.info(f"Processing {len(answers)} answers")
            all_ratings = await asyncio.gather(
                *[generate_ratings_stage(answer.id) for answer in answers]
            )
            db.add_all(all_ratings)
            await db.flush()
            await db.commit()
        logging.info(f"Processed {len(answers)} answers")
    stage_4_end_time = time.time()

    times = {
        "stage_1_time": stage_1_end_time - stage_1_start_time,
        "stage_2_time": stage_2_end_time - stage_2_start_time,
        "stage_3_time": stage_3_end_time - stage_3_start_time,
        "stage_4_time": stage_4_end_time - stage_4_start_time,
    }
    logging.info(f"Process completed in {times}")
    return times


async def process_all_paragraphs_s2s_optimized(batch_size=5):
    async def producer(queue, get_items_func, process_func):
        while True:
            async with async_session() as db:
                items = await get_items_func(db, batch_size)
                if not items:
                    break
                for item in items:
                    await queue.put(item)
            await asyncio.sleep(0)  # Allow other coroutines to run

    async def consumer(queue, process_func, commit_func):
        while True:
            item = await queue.get()
            if item is None:
                break
            result = await process_func(item)
            await commit_func(result)
            queue.task_done()

    @asynccontextmanager
    async def stage_context(name):
        start_time = time.time()
        logging.info(f"Starting {name}")
        try:
            yield
        finally:
            end_time = time.time()
            logging.info(f"{name} completed in {end_time - start_time:.2f} seconds")

    async def run_stage(name, get_items_func, process_func, num_consumers=3):
        queue = asyncio.Queue(maxsize=batch_size * 2)

        async def commit_results(db, results):
            db.add_all(results)
            await db.flush()
            await db.commit()

        async with stage_context(name):
            producer_task = asyncio.create_task(
                producer(queue, get_items_func, process_func)
            )
            consumer_tasks = [
                asyncio.create_task(
                    consumer(
                        queue,
                        process_func,
                        lambda result: commit_results(async_session(), result),
                    )
                )
                for _ in range(num_consumers)
            ]

            await producer_task
            for _ in range(num_consumers):
                await queue.put(None)
            await asyncio.gather(*consumer_tasks)

    stages = [
        (
            "Generate Questions",
            get_next_unprocessed_paragraphs,
            generate_questions_stage,
        ),
        ("Filter Questions", get_next_unfiltered_questions, filter_questions_stage),
        ("Generate Answers", get_next_unprocessed_questions, generate_answers_stage),
        ("Generate Ratings", get_next_unprocessed_answers, generate_ratings_stage),
    ]

    for name, get_items_func, process_func in stages:
        await run_stage(name, get_items_func, process_func)

    logging.info("All stages completed")


async def start_background_process_s2s(batch_size=64):
    try:
        await process_all_paragraphs_s2s_optimized(batch_size)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))


if __name__ == "__main__":
    asyncio.run(start_background_process_s2s(64))
