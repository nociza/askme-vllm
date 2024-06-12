import pandas as pd
import ultraimport
from fleecekmbackend.core.config import DATABASE_URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import asyncio
import nest_asyncio

generate_and_rate_answers = ultraimport(
    "__dir__/../lib/llm_utils.py", "generate_and_rate_answers"
)

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Create the database engine and session
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def process_batch(input_file, output_file):
    data_df = pd.read_csv(input_file)
    questions = (
        data_df[["id_question", "paragraph_id", "text_question"]]
        .rename(
            columns={
                "id_question": "id",
                "paragraph_id": "paragraph_id",
                "text_question": "text",
            }
        )
        .to_dict(orient="records")
    )

    async with async_session() as session:
        results_df = await generate_and_rate_answers(session, questions)
    results_df.to_csv(output_file, index=False)


if __name__ == "__main__":
    import sys

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    asyncio.run(process_batch(input_file, output_file))
