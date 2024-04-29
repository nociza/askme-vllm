import asyncio
import logging
from typing import List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import distinct, func, select, delete
from fleecekmbackend.db.ctl import async_session, engine
from fleecekmbackend.db.helpers import get_random_unprocessed_paragraph, get_next_unprocessed_paragraphs
from fleecekmbackend.services.dataset.fleece_qa import process_paragraph, process_paragraph_with_retry
from fleecekmbackend.db.models import Paragraph, Question, Answer, Rating, Author

async def process_all_pages():
    while True:
        async with async_session() as db:
            # Check if there are any unprocessed paragraphs
            unprocessed_paragraph_exists = await db.scalar(
                select(func.count(Paragraph.id)).where(Paragraph.processed == -1)
            )

            if not unprocessed_paragraph_exists:
                logging.info("All paragraphs have been processed. Stopping the process.")
                break

            try:
                current_paragraph = await get_random_unprocessed_paragraph(db)
                print(f"current_paragraph: {current_paragraph}")

                while current_paragraph != -1:
                    try:
                        logging.info(f"Processing page {current_paragraph.page_name}...")
                        generated_questions, generated_answers, generated_ratings = await process_paragraph(db, current_paragraph)
                        logging.info(f"generated_questions: {generated_questions}")
                        logging.info(f"generated_answers: {generated_answers}")
                        logging.info(f"generated_ratings: {generated_ratings}")
                        current_paragraph = await get_random_unprocessed_paragraph(db)
                    except Exception as e:
                        logging.error(f"Error processing page {current_paragraph.page_name}")
                        logging.error(str(e))
                        current_paragraph = await get_random_unprocessed_paragraph(db)

                logging.info("All pages processed successfully.")
            except Exception as e:
                logging.error(f"Error occurred in process_all_pages: {str(e)}")
                # Wait for a short duration before retrying
                await asyncio.sleep(5)

async def process_all_pages_parallel(batch_size=5):
    while True:
        async with async_session() as db:
            # Check if there are any unprocessed paragraphs
            unprocessed_paragraph_exists = await db.scalar(
                select(func.count(Paragraph.id)).where(Paragraph.processed == -1)
            )
            if not unprocessed_paragraph_exists:
                logging.info("All paragraphs have been processed. Stopping the process.")
                break

            try:
                # Get a batch of unprocessed paragraphs
                paragraphs = await get_next_unprocessed_paragraphs(db, batch_size)
                if not paragraphs:
                    logging.info("No unprocessed paragraphs found. Stopping the process.")
                    break

                # Process paragraphs in parallel
                tasks = []
                for paragraph in paragraphs:
                    task = asyncio.create_task(process_paragraph_with_retry(db, paragraph))
                    tasks.append(task)

                await asyncio.gather(*tasks)

            except Exception as e:
                logging.error(f"Error occurred in process_all_pages_parallel: {str(e)}")

            # Wait for a short duration before processing the next batch
            await asyncio.sleep(5)


async def start_background_process():
    try:
        await process_all_pages_parallel(1)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))
