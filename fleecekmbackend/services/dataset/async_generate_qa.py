import logging

from typing import List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import distinct, func, select

from fleecekmbackend.services.dataset.fleece_qa import process_paragraphs
from fleecekmbackend.db.models import Paragraph, Question, Answer, Rating, Author

async def process_all_pages(db: AsyncSession):
    try:
        last_processed_page = await db.scalar(select(func.max(Paragraph.page_name)))
        if last_processed_page is None:
            last_processed_page = ""
        
        query = select(Paragraph.page_name).distinct().where(Paragraph.page_name > last_processed_page).order_by(Paragraph.page_name)
        page_names = await db.scalars(query)
        
        total_pages = await db.scalar(select(func.count(distinct(Paragraph.page_name))).where(Paragraph.page_name > last_processed_page))
        processed_pages = 0

        for page_name in page_names:
            try:
                async with db.begin():
                    paragraphs = await db.scalars(select(Paragraph).filter(Paragraph.page_name == page_name).order_by(Paragraph.within_page_order))
                    generated_questions, generated_answers, generated_ratings = await process_paragraphs(db, paragraphs.all())
                    processed_pages += 1
                    logging.info(f"Processed page {processed_pages}/{total_pages}")

            except Exception as e:
                logging.error(f"Error processing page {page_name}")
                logging.error(str(e))
                await db.rollback()

        logging.info("All pages processed successfully.")
    except Exception as e:
        logging.error("Error in process_all_pages function:")
        logging.error(str(e))

async def test_process_all_pages(db: AsyncSession):
    # Clean up the database
    await db.execute(Rating.__table__.delete())
    await db.execute(Answer.__table__.delete())
    await db.execute(Question.__table__.delete())
    await db.execute(Author.__table__.delete())
    await db.execute(Paragraph.__table__.delete())
    await db.commit()

    # Create test data
    paragraphs = [
        Paragraph(page_name="Page 1", within_page_order=1, text="Paragraph 1"),
        Paragraph(page_name="Page 1", within_page_order=2, text="Paragraph 2"),
        Paragraph(page_name="Page 2", within_page_order=1, text="Paragraph 3"),
        Paragraph(page_name="Page 2", within_page_order=2, text="Paragraph 4"),
    ]
    db.add_all(paragraphs)
    await db.commit()

    # Run the process_all_pages function
    await process_all_pages(db)

    # Check the results
    questions = await db.scalars(select(Question))
    answers = await db.scalars(select(Answer))
    ratings = await db.scalars(select(Rating))

    assert len(questions.all()) > 0
    assert len(answers.all()) > 0
    assert len(ratings.all()) > 0

    # Run the process_all_pages function again to check for duplicates
    await process_all_pages(db)

    # Check that no duplicates were created
    questions = await db.scalars(select(Question))
    answers = await db.scalars(select(Answer))
    ratings = await db.scalars(select(Rating))

    assert len(questions.all()) == questions.distinct().count()
    assert len(answers.all()) == answers.distinct().count()
    assert len(ratings.all()) == ratings.distinct().count()

    logging.info("Test passed successfully.")