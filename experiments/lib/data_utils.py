# common_functions.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
import pandas as pd
from sqlalchemy import case, func
from fleecekmbackend.db.models import Author, Answer, Question, Paragraph, Rating
from fleecekmbackend.core.config import DATABASE_URL

# Setting up the async engine and session
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def fetch_paragraph_data(sample_size=10000):
    async with async_session() as session:
        paragraph_query = (
            select(Paragraph.id, Paragraph.text)
            .order_by(func.random())
            .limit(sample_size)
        )
        paragraph_results = await session.execute(paragraph_query)
        paragraphs = paragraph_results.fetchall()
        paragraph_ids = [p.id for p in paragraphs]

        question_query = select(
            Question.id, Question.text, Question.author_id, Question.paragraph_id
        ).where(Question.paragraph_id.in_(paragraph_ids))
        question_results = await session.execute(question_query)
        questions = question_results.fetchall()

        author_ids = list(set([q.author_id for q in questions]))

        author_query = select(Author.id, Author.model, Author.username).where(
            Author.id.in_(author_ids)
        )
        author_results = await session.execute(author_query)
        authors = author_results.fetchall()

        return paragraphs, questions, authors


def prepare_dataframe(paragraphs, questions, authors):
    paragraph_df = pd.DataFrame(paragraphs, columns=["paragraph_id", "paragraph_text"])
    question_df = pd.DataFrame(
        questions, columns=["question_id", "question_text", "author_id", "paragraph_id"]
    )
    author_df = pd.DataFrame(
        authors, columns=["author_id", "author_model", "author_username"]
    )

    merged_df = question_df.merge(paragraph_df, on="paragraph_id").merge(
        author_df, on="author_id"
    )

    return merged_df


async def fetch_and_prepare_data(sample_size=10000):
    paragraphs, questions, authors = await fetch_paragraph_data(sample_size)
    data_df = prepare_dataframe(paragraphs, questions, authors)
    return data_df
