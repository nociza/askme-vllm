import asyncio
import time
import pandas as pd
import logging
from datetime import datetime
from tqdm.notebook import tqdm
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from fleecekmbackend.db.models import Question, Paragraph, Answer, Rating
from fleecekmbackend.db.helpers import create_author_if_not_exists
from fleecekmbackend.core.utils.llm import (
    llm_safe_request,
    randwait,
    generate_prompts_from_template,
)
from fleecekmbackend.services.dataset.fleece_qa import (
    generate_fact_with_context,
    generate_answer_rating,
)
from fleecekmbackend.core.config import DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

WAIT = 2
MAX_ATTEMPTS = 3
MODEL = "meta-llama/Meta-Llama-3-70B-Instruct"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = [
    "<|begin_of_text|><|start_header_id|>user<|end_header_id|>\n\n",
    "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n",
]


async def generate_answer(
    db: AsyncSession,
    question: dict,
    setting: str = "ic",
    max_attempts: int = MAX_ATTEMPTS,
    model: str = MODEL,
    service: str = "gpublaze",
    prompt_type: str = "short",
):
    if setting == "ic":
        paragraph = await db.get(Paragraph, question["paragraph_id"])
        _, fact = generate_fact_with_context(paragraph)
        context_prompt = f"Using this fact: {fact} \n\n "
    else:
        context_prompt = ""

    if prompt_type == "short":
        prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in the shortest possible manner: {QUESTION}\n{PROMPT_SUFFIX}"
    elif prompt_type == "5_words":
        prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in under 5 words: {QUESTION}\n{PROMPT_SUFFIX}"
    else:
        raise ValueError(f"Unknown prompt type: {prompt_type}")

    prompt, template = generate_prompts_from_template(
        prompt_template,
        {
            "CONTEXT_PROMPT": context_prompt,
            "QUESTION": question["text"],
            "PROMPT_PREFIX": PROMPT_PREFIX,
            "PROMPT_SUFFIX": PROMPT_SUFFIX,
        },
    )
    author_id = await create_author_if_not_exists(template, model)

    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        time.sleep(randwait(WAIT))
        output = llm_safe_request(prompt, model, STOP, service=service)
        answer_text = output["choices"][0]["message"]["content"].strip()

        if answer_text:
            answer = Answer(
                question_id=question["id"],
                author_id=author_id,
                setting=setting,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                text=answer_text,
            )
            logging.info(f"Generated answer: {answer.text}")
            db.add(answer)
            await db.flush()
            await db.refresh(answer, ["id"])
            return {
                "answer_id": answer.id,
                "answer_text": answer.text,
                "author_id": author_id,
                "question_id": question["id"],
            }

    raise Exception(
        f"Cannot generate a valid answer after {max_attempts} attempts. \n Question: {question['text']}"
    )


async def generate_and_rate_answers(
    db: AsyncSession,
    questions: list[dict],
):
    df = pd.DataFrame(
        columns=[
            "question_id",
            "short_answer_id",
            "short_answer_text",
            "five_word_answer_id",
            "five_word_answer_text",
            "short_answer_rating_id",
            "five_word_answer_rating_id",
            "short_answer_rating_score",
            "five_word_answer_rating_score",
            "short_answer_rating_rationale",
            "five_word_answer_rating_rationale",
        ]
    )
    for question in tqdm(questions):
        short_answer = await generate_answer(db, question, prompt_type="short")
        five_word_answer = await generate_answer(db, question, prompt_type="5_words")

        short_answer_id = short_answer["answer_id"]
        five_word_answer_id = five_word_answer["answer_id"]

        short_answer_rating_id = await generate_answer_rating(
            db, question["id"], short_answer_id
        )
        five_word_answer_rating_id = await generate_answer_rating(
            db, question["id"], five_word_answer_id
        )

        short_answer_rating = await db.get(Rating, short_answer_rating_id)
        five_word_answer_rating = await db.get(Rating, five_word_answer_rating_id)

        short_answer_rating_score = short_answer_rating.value
        five_word_answer_rating_score = five_word_answer_rating.value
        short_answer_rating_rationale = short_answer_rating.text
        five_word_answer_rating_rationale = five_word_answer_rating.text

        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    [
                        {
                            "question_id": question["id"],
                            "short_answer_id": short_answer_id,
                            "short_answer_text": short_answer["answer_text"],
                            "five_word_answer_id": five_word_answer_id,
                            "five_word_answer_text": five_word_answer["answer_text"],
                            "short_answer_rating_id": short_answer_rating_id,
                            "five_word_answer_rating_id": five_word_answer_rating_id,
                            "short_answer_rating_score": short_answer_rating_score,
                            "five_word_answer_rating_score": five_word_answer_rating_score,
                            "short_answer_rating_rationale": short_answer_rating_rationale,
                            "five_word_answer_rating_rationale": five_word_answer_rating_rationale,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    return df
