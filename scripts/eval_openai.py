import asyncio
from sqlalchemy import func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from dotenv import dotenv_values
from fleecekmbackend.services.dataset.fleece_qa import (
    generate_answer,
    generate_answer_rating,
)
from fleecekmbackend.db.models import Question, Answer, Paragraph
from fleecekmbackend.core.config import DATABASE_URL

engine = create_async_engine(DATABASE_URL)
AsyncSession = sessionmaker(engine, class_=AsyncSession)

MODEL = "gpt-4-turbo"


async def process_questions():
    async with AsyncSession() as db:
        results = await db.execute(select(Question).order_by(func.random()).limit(50))
        questions = results.scalars().all()

        for question in questions:
            answer_id_ic = await generate_answer(
                db, question.id, "ic", model=MODEL, service="openai"
            )
            answer_id_zs = await generate_answer(
                db, question.id, "zs", model=MODEL, service="openai"
            )

            print(
                "Zero-shot answer: ",
                answer_id_ic,
                "\n",
                "In-context answer: ",
                answer_id_zs,
            )

            if answer_id_ic:
                result_ic = await generate_answer_rating(db, question.id, answer_id_ic)
                print("In-context Rating: ", result_ic)
            if answer_id_zs:
                result_zs = await generate_answer_rating(db, question.id, answer_id_zs)
                print("Zeroshot Rating: ", result_zs)


async def main():
    await process_questions()


if __name__ == "__main__":
    asyncio.run(main())
