import time
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from fleecekmbackend.db.models import (
    Paragraph,
    Question,
    Answer,
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


async def generate_answer(
    db: AsyncSession,
    question_id: int,
    setting: str = None,
    max_attempts: int = MAX_ATTEMPTS,
    model: str = MODEL,
    service: str = "gpublaze",
    flush: bool = True,
):
    try:
        # process prompt template
        question = await db.get(Question, question_id)

        prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in a succinct manner: {QUESTION}\n{PROMPT_SUFFIX}"

        if setting == "ic":
            paragraph = await db.get(Paragraph, question.paragraph_id)
            _, fact = generate_fact_with_context(paragraph)
            context_prompt = f"Using this fact: {fact} \n\n "
        elif setting == "zs":
            context_prompt = ""
        else:
            raise Exception("Invalid setting")

        prompt, template = generate_prompts_from_template(
            prompt_template,
            {
                "CONTEXT_PROMPT": context_prompt,
                "QUESTION": question.text,
                "PROMPT_PREFIX": PROMPT_PREFIX,
                "PROMPT_SUFFIX": PROMPT_SUFFIX,
            },
        )
        author_id = await create_author_if_not_exists(template, model)

        # main loop
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            time.sleep(randwait(WAIT))
            output = await llm_safe_request_async(prompt, model, STOP, service=service)
            answer_text = output["choices"][0]["message"]["content"].strip()

            if answer_text:
                answer = Answer(
                    question_id=question.id,
                    author_id=author_id,
                    setting=setting,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    text=answer_text,
                )
                logging.debug(f"Generated answer: {answer.text}")
                if flush:
                    db.add(answer)
                    await db.flush()
                    await db.refresh(answer, ["id"])
                    return answer.id
                else:
                    return answer

        raise Exception(
            f"Cannot generate a valid answer after {max_attempts} attempts. \n Question: {question.text}"
        )
    except Exception as e:
        logging.error(f"An error occurred at generate_answer: {e}")
