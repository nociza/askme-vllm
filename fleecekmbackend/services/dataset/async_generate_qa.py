import asyncio
import logging
from typing import List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import distinct, func, select, delete
from fleecekmbackend.db.ctl import async_session, engine
from fleecekmbackend.db.helpers import get_random_unprocessed_paragraph
from fleecekmbackend.services.dataset.fleece_qa import process_paragraph
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

async def test_process_all_pages():
    async with AsyncSession(engine) as db:
        # Clean up the database
        print("Cleaning up the database...")

        await db.execute(delete(Rating))
        await db.execute(delete(Answer))
        await db.execute(delete(Question))
        await db.execute(delete(Author))
        await db.execute(delete(Paragraph))
        await db.commit()

        print("Database cleaned successfully.")

        # Create test data
        paragraphs = [
            Paragraph(page_name="Page 1", within_page_order=1, text="Paragraph 1"),
            # Paragraph(page_name="Page 1", within_page_order=2, text="Paragraph 2"),
            # Paragraph(page_name="Page 2", within_page_order=1, text="Paragraph 3"),
            # Paragraph(page_name="Page 2", within_page_order=2, text="Paragraph 4"),
        ]
        for paragraph in paragraphs:
            db.add(paragraph)
        await db.commit()

        print("Test data created successfully.")

        # Run the process_all_pages function
        await process_all_pages(db)

        print("All pages processed successfully.")

        # Check the results
        questions = (await db.scalars(select(Question))).all()
        answers = (await db.scalars(select(Answer))).all()
        ratings = (await db.scalars(select(Rating))).all()

        print(f"Questions: {len(questions)}")

        assert len(questions) > 0
        assert len(answers) > 0
        assert len(ratings) > 0

        print("Results checked successfully.")

        # Run the process_all_pages function again to check for duplicates
        try:
            artifacts = await process_all_pages(db)
            print("Artifacts:", artifacts)
        except Exception as e:
            print("Error in process_all_pages function:")
            print(str(e))
        finally:
            print("All pages processed successfully.")

            # Check that no duplicates were created
            questions = await db.scalars(select(Question))
            answers = await db.scalars(select(Answer))
            ratings = await db.scalars(select(Rating))

            assert len(questions.all()) == questions.distinct().count()
            assert len(answers.all()) == answers.distinct().count()
            assert len(ratings.all()) == ratings.distinct().count()

            print("Test passed successfully.")

async def start_background_process():
    try:
        await process_all_pages()
    except Exception as e:
        logging.error("Error in background process:")
        logging.error(str(e))

async def start_background_process_test():
    try:
        await test_process_all_pages()
    except Exception as e:
        logging.error("Error in background process test:")
        logging.error(str(e))