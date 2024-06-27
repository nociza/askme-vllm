import re
import time
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from fleecekmbackend.db.models import (
    Paragraph,
    Question,
    Answer,
    Rating,
)
from fleecekmbackend.db.helpers import create_author_if_not_exists
from fleecekmbackend.core.utils.llm import (
    llm_safe_request,
    llm_safe_request_async,
    randwait,
    generate_prompts_from_template,
)
from fleecekmbackend.services.dataset.common import generate_fact_with_context
from fleecekmbackend.core.config import (
    WAIT,
    MODEL,
    STOP,
    PROMPT_PREFIX,
    PROMPT_SUFFIX,
    MAX_ATTEMPTS,
    LOGGING_LEVEL,
)

logging.basicConfig(
    level=LOGGING_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def generate_answer_rating(
    db: AsyncSession,
    answer_id: int,
    max_attempts: int = MAX_ATTEMPTS,
    model: str = MODEL,
    service: str = "gpublaze",
    flush: bool = True,
):
    try:
        prompt_template = "{PROMPT_PREFIX}Based on this fact: \n\n `{REFERENCE}` \n\n Rate the following answer to the question - Question: `{QUESTION}` \n\n Answer: `{ANSWER}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n{PROMPT_SUFFIX}"

        answer = await db.get(Answer, answer_id)
        question = await db.get(Question, answer.question_id)
        paragraph = await db.get(Paragraph, question.paragraph_id)

        _, reference = generate_fact_with_context(paragraph)

        prompt, template = generate_prompts_from_template(
            prompt_template,
            {
                "REFERENCE": reference,
                "QUESTION": question.text,
                "ANSWER": answer.text,
                "PROMPT_PREFIX": PROMPT_PREFIX,
                "PROMPT_SUFFIX": PROMPT_SUFFIX,
            },
        )

        author_id = await create_author_if_not_exists(template, model)

        logging.debug(f"Author ID: {author_id}")

        # main loop
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            time.sleep(randwait(WAIT))
            output = await llm_safe_request_async(prompt, model, STOP, service=service)
            rating_raw = output["choices"][0]["message"]["content"].strip()

            if re.search(r"Rationale:", rating_raw, re.I) and re.search(
                r"[0-5]", rating_raw
            ):
                score = int(re.search(r"[0-5]", rating_raw).group())
                rationale = "".join(rating_raw.split("Rationale:", re.I)[1:]).strip()

                logging.debug(f"Score: {score}, Rationale: {rationale}")

                rating = Rating(
                    text=rationale,
                    value=score,
                    answer_id=answer_id,
                    author_id=author_id,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                logging.debug(
                    f"Generated rating: {rating.value} for answer: {answer.text} with rationale: {rating.text}"
                )
                if flush:
                    db.add(rating)
                    await db.flush()
                    await db.refresh(rating, ["id"])
                    logging.debug(
                        f"Generated rating: {rating.value} for answer: {answer.text} with rationale: {rating.text}, id: {rating.id}"
                    )
                    rating_id = rating.id
                    return rating_id
                else:
                    return rating

        raise Exception(
            f"Cannot rate answers to the correct format after {max_attempts} attempts."
        )
    except Exception as e:
        logging.error(f"An error occurred at generate_answer_rating: {e}")
