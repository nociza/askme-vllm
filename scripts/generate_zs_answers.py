import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from tqdm import tqdm

from fleecekmbackend.services.dataset.fleece_qa import (
    generate_answer,
    generate_answer_rating,
)
from fleecekmbackend.db.models import Question, Answer, Rating
from fleecekmbackend.core.config import DATABASE_URL

# Database connection and session setup
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSession = sessionmaker(engine, class_=AsyncSession)


async def process_questions(db):
    async with db() as session:
        # Fetch all questions
        result = await session.execute(select(Question))
        questions = result.scalars().all()

        # Wrap the loop with tqdm for a progress bar
        for question in tqdm(questions, desc="Processing Questions"):
            try:
                # Generate zero-shot answer
                answer_id = await generate_answer(session, question.id, setting="zs")
                if answer_id:
                    # Generate rating for the answer
                    await generate_answer_rating(session, question.id, answer_id)
            except Exception as e:
                logging.error(f"Failed processing question ID {question.id}: {str(e)}")


async def main():
    # Run processing for all questions
    await process_questions(AsyncSession)


if __name__ == "__main__":
    asyncio.run(main())
