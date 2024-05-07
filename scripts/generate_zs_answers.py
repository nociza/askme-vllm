import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.future import select
from sqlalchemy import and_, or_
from tqdm import tqdm

from fleecekmbackend.services.dataset.fleece_qa import (
    generate_answer,
    generate_answer_rating,
)
from fleecekmbackend.db.models import Question, Answer
from fleecekmbackend.core.config import DATABASE_URL

# Database connection and session setup
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSession = sessionmaker(engine, class_=AsyncSession)


async def process_questions(db):
    async with db() as session:
        # Create an alias for Answer to use in the LEFT JOIN
        answer_alias = aliased(Answer)

        # Fetch questions that do not have a 'zs' answer
        result = await session.execute(
            select(Question)
            .outerjoin(
                answer_alias,
                and_(
                    Question.id == answer_alias.question_id,
                    answer_alias.setting == "zs",
                ),
            )
            .filter(answer_alias.id == None)
        )
        questions = result.scalars().all()

        # Process each question with tqdm
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
