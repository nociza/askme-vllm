import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from statsmodels.api import OLS
import pandas as pd
from sqlalchemy import and_

from fleecekmbackend.db.models import Author, Answer, Rating
from fleecekmbackend.core.config import DATABASE_URL

# Setting up the async engine and session
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def fetch_data():
    async with async_session() as session:
        # Select answers authored by 'meta-llama/Meta-Llama-3-70B-Instruct' with their ratings
        results = await session.execute(
            select(Answer, Rating)
            .join(Rating, Answer.id == Rating.answer_id)
            .join(Author, Author.id == Answer.author_id)
            .where(Author.model == "meta-llama/Meta-Llama-3-70B-Instruct")
        )
        return results.all()


def prepare_data(data):
    # Create a DataFrame from the fetched data
    df = pd.DataFrame(
        [
            {"setting": answer.setting, "rating_value": rating.value}
            for answer, rating in data
        ]
    )

    # Convert setting to a categorical variable
    df["setting"] = df["setting"].astype("category").cat.codes
    return df


async def perform_regression(df):
    # Performing regression
    X = df[["setting"]]  # Independent variable
    y = df["rating_value"]  # Dependent variable
    model = OLS(y, X).fit()
    return model.summary()


async def main():
    data = await fetch_data()
    if data:
        df = prepare_data(data)
        regression_result = await perform_regression(df)
        print(regression_result)
    else:
        print("No data found for specified author.")


# Run the main coroutine
asyncio.run(main())
