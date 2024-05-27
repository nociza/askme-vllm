# Imports
from sqlalchemy import func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from fleecekmbackend.db.models import Paragraph, Author, Question, Answer, Rating
from fleecekmbackend.core.config import DATABASE_URL
import pandas as pd

# Setting up the async engine and session
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# Function to fetch sample data
async def fetch_and_prepare_data(sample_size=10000):
    async with async_session() as session:
        # Query to fetch sample questions with turns = 'single'
        question_query = (
            select(Question)
            .where(Question.turns == "single")
            .order_by(func.random())
            .limit(sample_size)
        )

        question_results = await session.execute(question_query)
        questions = question_results.scalars().all()

        # Extract question IDs for further queries
        question_ids = [q.id for q in questions]

        # Query to fetch corresponding paragraphs
        paragraph_query = select(Paragraph).where(
            Paragraph.id.in_([q.paragraph_id for q in questions])
        )

        paragraph_results = await session.execute(paragraph_query)
        paragraphs = paragraph_results.scalars().all()

        # Query to fetch corresponding authors
        author_query = select(Author).where(
            Author.id.in_([q.author_id for q in questions])
        )

        author_results = await session.execute(author_query)
        authors = author_results.scalars().all()

        # Query to fetch corresponding answers
        answer_query = select(Answer).where(Answer.question_id.in_(question_ids))

        answer_results = await session.execute(answer_query)
        answers = answer_results.scalars().all()

        # Query to fetch corresponding ratings
        answer_ids = [a.id for a in answers]
        rating_query = select(Rating).where(Rating.answer_id.in_(answer_ids))

        rating_results = await session.execute(rating_query)
        ratings = rating_results.scalars().all()

        # Convert results to DataFrames
        question_df = pd.DataFrame([q.__dict__ for q in questions])
        paragraph_df = pd.DataFrame([p.__dict__ for p in paragraphs])
        author_df = pd.DataFrame([a.__dict__ for a in authors])
        answer_df = pd.DataFrame([a.__dict__ for a in answers])
        rating_df = pd.DataFrame([r.__dict__ for r in ratings])

        # Keep only one answer per question randomly
        answer_df = (
            answer_df.groupby("question_id")
            .apply(lambda x: x.sample(1))
            .reset_index(drop=True)
        )

        # Merge DataFrames
        merged_df = question_df.merge(
            paragraph_df,
            left_on="paragraph_id",
            right_on="id",
            suffixes=("_question", "_paragraph"),
        )
        merged_df = merged_df.merge(
            author_df, left_on="author_id", right_on="id", suffixes=("", "_author")
        )
        merged_df = merged_df.merge(
            answer_df,
            left_on="id_question",
            right_on="question_id",
            suffixes=("", "_answer"),
        )
        merged_df = merged_df.merge(
            rating_df,
            left_on="id_answer",
            right_on="answer_id",
            suffixes=("", "_rating"),
        )

        return merged_df


# Define function to fetch question and paragraph information
async def fetch_question_paragraph_info(question_ids):
    async with async_session() as session:
        question_query = select(Question).where(Question.id.in_(question_ids))
        paragraph_query = select(Paragraph).where(Paragraph.id.in_(question_ids))

        question_results = await session.execute(question_query)
        paragraph_results = await session.execute(paragraph_query)

        questions = question_results.scalars().all()
        paragraphs = paragraph_results.scalars().all()

        question_df = pd.DataFrame([q.__dict__ for q in questions])
        paragraph_df = pd.DataFrame([p.__dict__ for p in paragraphs])

        return question_df, paragraph_df
