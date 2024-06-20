import asyncio
import logging
from sqlalchemy import func, select, update
from fleecekmbackend.db.ctl import async_session, engine
from fleecekmbackend.db.helpers import (
    get_random_unprocessed_paragraphs,
    get_next_unprocessed_paragraphs,
)
from fleecekmbackend.services.dataset.fleece_qa import (
    process_paragraph,
    process_paragraph_with_retry,
)
from fleecekmbackend.db.models import Paragraph

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def process_all_pages():
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
                            await process_paragraph(db, current_paragraph)
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


async def process_all_pages_parallel(batch_size=5):
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
                        process_paragraph_with_retry(db, paragraph)
                    )
                    tasks.append(task)

                await asyncio.gather(*tasks)

            except Exception as e:
                logging.error(f"Error occurred in process_all_pages_parallel: {str(e)}")

            await asyncio.sleep(5)


async def start_background_process():
    try:
        await process_all_pages_parallel(1)
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))
