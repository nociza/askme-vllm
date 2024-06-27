import asyncio
import logging
import time
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from collections import deque
from contextlib import asynccontextmanager

from fleecekmbackend.db.ctl import async_session, create_tables_if_not_exist
from fleecekmbackend.db.helpers import (
    get_next_unfiltered_questions,
    get_next_unprocessed_paragraphs,
    get_next_unprocessed_questions,
    get_next_unprocessed_answers,
    get_unfiltered_questions_count,
    get_unprocessed_answers_count,
    get_unprocessed_paragraphs_count,
    get_unprocessed_questions_count,
    load_csv_data_all,
    load_csv_data_rand_n,
    load_csv_data_top_n,
)
from fleecekmbackend.db.models import Paragraph, Question, Answer, Rating
from fleecekmbackend.services.dataset.questions import (
    filter_questions,
    generate_questions_single_turn,
)
from fleecekmbackend.services.dataset.answers import generate_answer
from fleecekmbackend.services.dataset.ratings import generate_answer_rating
from fleecekmbackend.core.config import DATASET_PATH, LOGGING_LEVEL

logging.basicConfig(
    level=LOGGING_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def generate_questions_stage(paragraph: Paragraph) -> List[Question]:
    try:
        questions = await generate_questions_single_turn(paragraph)
        paragraph.processed = True
        return questions + [paragraph]
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


async def generate_answers_stage(question: Question) -> List[Answer]:
    objects2update = []
    async with async_session() as db:
        try:
            for setting in ["zs", "ic"]:
                answer = await generate_answer(db, question.id, setting, flush=False)
                objects2update.append(answer)
            question.processed = True
            objects2update.append(question)
            return objects2update
        except Exception as e:
            logging.error(f"Error generating answers for question: {question.id}")
            logging.error(str(e))
            raise


async def generate_ratings_stage(answer: Answer) -> Rating:
    async with async_session() as db:
        try:
            rating = await generate_answer_rating(db, answer.id, flush=False)
            answer.processed = True
            return [rating, answer]
        except Exception as e:
            logging.error(f"Error generating rating for answer: {answer.id}")
            logging.error(str(e))
            raise


async def process_all_paragraphs_s2s(batch_size=5):
    # Stage 1: Generate Questions
    logging.info("Starting stage 1: Generate Questions")
    stage_1_start_time = time.time()
    total_paragraphs = await get_unprocessed_paragraphs_count()
    print(total_paragraphs)
    with tqdm(total=total_paragraphs, desc="Stage 1: Generate Questions") as pbar:
        while True:
            async with async_session() as db:
                paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
                if not paragraphs:
                    logging.info(
                        "No unprocessed paragraphs found. Moving to next stage."
                    )
                    break
                all_questions = await tqdm_asyncio.gather(
                    *[generate_questions_stage(paragraph) for paragraph in paragraphs],
                    desc="Processing paragraphs",
                )
                if all_questions:
                    all_questions = [
                        q for questions in all_questions for q in questions
                    ]
                    db.add_all(all_questions)
                    await db.flush()
                    await db.commit()
                pbar.update(len(paragraphs))
    stage_1_end_time = time.time()

    # Stage 2: Filter Questions
    logging.info("Starting stage 2: Filter Questions")
    stage_2_start_time = time.time()
    total_questions = await get_unfiltered_questions_count()
    with tqdm(total=total_questions, desc="Stage 2: Filter Questions") as pbar:
        while True:
            async with async_session() as db:
                questions = await get_next_unfiltered_questions(db, batch_size)
                if not questions:
                    logging.info(
                        "No unprocessed questions found. Moving to next stage."
                    )
                    break
                filtered_questions = await filter_questions_stage(questions)
                db.add_all(filtered_questions)
                await db.flush()
                await db.commit()
                pbar.update(len(questions))
    stage_2_end_time = time.time()

    # Stage 3: Generate Answers
    logging.info("Starting stage 3: Generate Answers")
    stage_3_start_time = time.time()
    total_questions = await get_unprocessed_questions_count()
    with tqdm(total=total_questions, desc="Stage 3: Generate Answers") as pbar:
        while True:
            async with async_session() as db:
                questions = await get_next_unprocessed_questions(db, batch_size)
                if not questions:
                    logging.info(
                        "No unprocessed questions found. Moving to next stage."
                    )
                    break
                all_answers = await tqdm_asyncio.gather(
                    *[generate_answers_stage(question) for question in questions],
                    desc="Processing questions",
                )
                all_answers = [a for answers in all_answers for a in answers]
                db.add_all(all_answers)
                await db.flush()
                await db.commit()
                pbar.update(len(questions))
    stage_3_end_time = time.time()

    # Stage 4: Generate Ratings
    logging.info("Starting stage 4: Generate Ratings")
    stage_4_start_time = time.time()
    total_answers = await get_unprocessed_answers_count()
    with tqdm(total=total_answers, desc="Stage 4: Generate Ratings") as pbar:
        while True:
            async with async_session() as db:
                answers = await get_next_unprocessed_answers(db, batch_size)
                if not answers:
                    logging.info("No unprocessed answers found. Finishing process.")
                    break
                all_ratings = await tqdm_asyncio.gather(
                    *[generate_ratings_stage(answer) for answer in answers],
                    desc="Processing answers",
                )
                all_ratings = [r for ratings in all_ratings for r in ratings]
                db.add_all(all_ratings)
                await db.flush()
                await db.commit()
                pbar.update(len(answers))
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
    async def producer(queue, get_items_func):
        while True:
            async with async_session() as db:
                items = await get_items_func(db, batch_size)
                if not items:
                    break
                for item in items:
                    await queue.put(item)
            await asyncio.sleep(0)  # Allow other coroutines to run
        # Signal end of production
        await queue.put(None)

    async def consumer(queue, process_func, commit_func):
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            result = await process_func(item)
            async with async_session() as db:
                await commit_func(db, result)
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

    async def run_stage(
        name, get_items_func, process_func, commit_func, num_consumers=3
    ):
        queue = asyncio.Queue(maxsize=batch_size * 2)
        async with stage_context(name):
            producer_task = asyncio.create_task(producer(queue, get_items_func))
            consumer_tasks = [
                asyncio.create_task(consumer(queue, process_func, commit_func))
                for _ in range(num_consumers)
            ]

            await producer_task
            await queue.join()
            for _ in range(num_consumers):
                await queue.put(None)
            await asyncio.gather(*consumer_tasks)

    async def commit_results(db, results):
        if isinstance(results, list):
            db.add_all(results)
        else:
            db.add(results)
        await db.flush()
        await db.commit()

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
        await run_stage(name, get_items_func, process_func, commit_results)

    logging.info("All stages completed")


async def start_background_process_s2s(batch_size=128):
    try:
        await process_all_paragraphs_s2s(batch_size)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))


async def main():
    await create_tables_if_not_exist()

    with open(DATASET_PATH, "r") as file:
        await load_csv_data_all(file)

    await start_background_process_s2s(128)


if __name__ == "__main__":
    asyncio.run(main())
