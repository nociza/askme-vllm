import re
import time
import logging
import asyncio
import traceback
from datetime import datetime
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from fleecekmbackend.db.models import (
    Paragraph,
    Question,
    RejectedQuestion,
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
    NUMQUESTIONS,
    MAX_ATTEMPTS,
    LOGGING_LEVEL,
)

logging.basicConfig(
    level=LOGGING_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


###################################################################################################
#                                        Combined Functions                                       #
###################################################################################################
async def generate_n_filter_questions_with_retry(
    db: AsyncSession,
    paragraph: Paragraph,
    k: int = NUMQUESTIONS,
    max_attempts: int = MAX_ATTEMPTS,
):
    try:
        # process prompt template
        time.sleep(randwait(WAIT))
        prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} additional short answer (DO NOT INCLUDE CHOICES) questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead. Remember to include relevant context in the question. \n\nExisting questions:\n{EXISTING_QUESTIONS}\n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
        context, fact = generate_fact_with_context(paragraph)
        _, template = generate_prompts_from_template(
            prompt_template,
            {
                "NUM_QUESTIONS": k,
                "EXISTING_QUESTIONS": "",
                "PARAGRAPH": fact,
                "PROMPT_PREFIX": PROMPT_PREFIX,
                "PROMPT_SUFFIX": PROMPT_SUFFIX,
            },
        )

        author_id = await create_author_if_not_exists(template, MODEL)

        logging.info(f"Generating questions for paragraph: {paragraph.id}")

        # helper function to generate questions
        def generate_or_regenerate_questions(existing_questions):
            existing = ""
            for i, q in enumerate(existing_questions):
                existing += f"{i+1}. {q}\n"
            prompt, _ = generate_prompts_from_template(
                prompt_template,
                {
                    "NUM_QUESTIONS": k,
                    "EXISTING_QUESTIONS": existing,
                    "PARAGRAPH": fact,
                    "PROMPT_PREFIX": PROMPT_PREFIX,
                    "PROMPT_SUFFIX": PROMPT_SUFFIX,
                },
            )
            logging.info(f"Prompt: {prompt}")
            time.sleep(randwait(WAIT))
            output = llm_safe_request(prompt, MODEL, STOP)
            logging.info(
                f"Generated questions: {output['choices'][0]['message']['content']}"
            )
            new_questions = [
                x[2:].strip()
                for x in output["choices"][0]["message"]["content"].strip().split("\n")
                if re.match(r"^[0-9]\.", x)
            ]
            return existing_questions + new_questions

        # main loop
        good_questions = []
        attempts = 0
        while len(good_questions) < k and attempts < max_attempts:
            attempts += 1
            questions = generate_or_regenerate_questions(good_questions)
            logging.info(f"Generated Questions {attempts}: {questions}")
            for q in questions:
                logging.info(f"Checking if answerable: {q}")
                q_is_answerable_ic = is_answerable(q, fact)
                q_is_answerable_zs = is_answerable(q)
                logging.info(
                    f"Answerable in IC: {q_is_answerable_ic}, Answerable in ZS: {q_is_answerable_zs}"
                )
                if q_is_answerable_ic and q_is_answerable_zs:
                    good_questions.append(q)
            logging.info(f"Good Questions {attempts}: {good_questions}")
        if len(good_questions) < k:
            logging.error(
                f"Failed to get {k} questions after {max_attempts} attempts, current number of questions: {len(good_questions)}, for fact: {fact}"
            )

        logging.info(f"Good Questions: {good_questions}")

        question_objs = []
        # Add questions to the database
        for q in good_questions:
            question = Question(
                paragraph_id=paragraph.id,
                scope="single-paragraph",
                text=q,
                context=context,
                author_id=author_id,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                upvote=0,
                downvote=0,
            )
            logging.info(f"Adding question: {question.text}")
            db.add(question)
            await db.flush()
            await db.refresh(question, ["id"])
            question_objs.append(question.id)
        return question_objs
    except Exception as e:
        logging.error(str(e))
        raise


# generate the questions with only one turn and reject all unanswerable questions
async def generate_n_filter_questions_single_turn(
    db: AsyncSession,
    paragraph: Paragraph,
    k: int = NUMQUESTIONS,
    flush: bool = True,
):
    try:
        prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} short answer questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead. Remember to include relevant context in the question. \n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
        context, fact = generate_fact_with_context(paragraph)
        prompt, template = generate_prompts_from_template(
            prompt_template,
            {
                "PARAGRAPH": fact,
                "PROMPT_PREFIX": PROMPT_PREFIX,
                "PROMPT_SUFFIX": PROMPT_SUFFIX,
                "NUM_QUESTIONS": k,
            },
        )

        author_id = await create_author_if_not_exists(template, MODEL)

        logging.info(f"Generating questions for paragraph: {paragraph.id}")

        async def generate_and_reject_unanswerable_questions():
            output = llm_safe_request(prompt, MODEL, STOP)
            logging.info(
                f"Generated questions: {output['choices'][0]['message']['content']}"
            )
            new_questions = [
                x[2:].strip()
                for x in output["choices"][0]["message"]["content"].strip().split("\n")
                if re.match(r"^[0-9]\.", x)
            ]
            good_questions = []
            rejected_questions = []

            async def check_question(q):
                logging.info(f"Checking if answerable: {q}")
                q_is_answerable_ic = await is_answerable_guided_choice(q, fact)
                q_is_answerable_zs = await is_answerable_guided_choice(q)
                logging.info(
                    f"Answerable in IC: {q_is_answerable_ic}, Answerable in ZS: {q_is_answerable_zs}"
                )
                if q_is_answerable_ic and q_is_answerable_zs:
                    good_questions.append(q)
                else:
                    rejected_questions.append(
                        RejectedQuestion(
                            paragraph_id=paragraph.id,
                            scope="single-paragraph",
                            text=q,
                            context=context,
                            author_id=author_id,
                            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            turns="single",
                            is_answerable_ic=q_is_answerable_ic,
                            is_answerable_zs=q_is_answerable_zs,
                        )
                    )

            await asyncio.gather(*[check_question(q) for q in new_questions])

            if flush:
                db.add_all(rejected_questions)
                await db.flush()
                return good_questions, None
            else:
                return good_questions, rejected_questions

        good_questions, rejected_questions = (
            await generate_and_reject_unanswerable_questions()
        )

        logging.info(f"Good Questions: {good_questions}")

        questions_to_add = [
            Question(
                paragraph_id=paragraph.id,
                scope="single-paragraph",
                text=q,
                context=context,
                author_id=author_id,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                upvote=0,
                downvote=0,
                turns="single",
            )
            for q in good_questions
        ]
        if flush:
            db.add_all(questions_to_add)
            await db.flush()
            question_objs = [question.id for question in questions_to_add]
            return question_objs
        else:
            return questions_to_add, rejected_questions

    except Exception as e:
        logging.error(str(e))
        logging.error(traceback.format_exc())
        raise "Error generating questions for paragraph" from e


###################################################################################################
#                                       Question Generation                                       #
###################################################################################################
async def generate_questions_single_turn(
    paragraph: Paragraph,
    k: int = NUMQUESTIONS,
) -> List[Question]:
    try:
        prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} short answer questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead. Remember to include relevant context in the question. \n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
        context, fact = generate_fact_with_context(paragraph)
        prompt, template = generate_prompts_from_template(
            prompt_template,
            {
                "PARAGRAPH": fact,
                "PROMPT_PREFIX": PROMPT_PREFIX,
                "PROMPT_SUFFIX": PROMPT_SUFFIX,
                "NUM_QUESTIONS": k,
            },
        )

        author_id = await create_author_if_not_exists(template, MODEL)

        logging.debug(f"Generating questions for paragraph: {paragraph.id}")

        output = await llm_safe_request_async(prompt, MODEL, STOP)
        logging.debug(
            f"Generated questions: {output['choices'][0]['message']['content']}"
        )
        new_questions = [
            x[2:].strip()
            for x in output["choices"][0]["message"]["content"].strip().split("\n")
            if re.match(r"^[0-9]\.", x)
        ]

        question_objects = [
            Question(
                paragraph_id=paragraph.id,
                scope="single-paragraph",
                text=q,
                context=context,
                author_id=author_id,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                upvote=0,
                downvote=0,
                turns="single",
            )
            for q in new_questions
        ]
        return question_objects

    except Exception as e:
        logging.error(str(e))
        logging.error(traceback.format_exc())
        raise Exception("Error generating questions for paragraph") from e


async def filter_questions(
    db: AsyncSession,
    questions: List[Question],
) -> List[Question]:
    updated_questions = []

    questions_by_paragraph = {}
    for q in questions:
        if q.paragraph_id not in questions_by_paragraph:
            questions_by_paragraph[q.paragraph_id] = []
        questions_by_paragraph[q.paragraph_id].append(q)

    async def check_questions_for_paragraph(paragraph_id, questions):
        paragraph = await db.get(Paragraph, paragraph_id)
        context, fact = generate_fact_with_context(paragraph)

        for q in questions:
            logging.debug(f"Checking if answerable: {q.text}")
            q_is_answerable_ic = await is_answerable_guided_choice(q.text, fact)
            q_is_answerable_zs = await is_answerable_guided_choice(q.text)
            logging.debug(
                f"Answerable in IC: {q_is_answerable_ic}, Answerable in ZS: {q_is_answerable_zs}"
            )
            if not q_is_answerable_ic or not q_is_answerable_zs:
                q.rejected = True
                q.is_answerable_ic = q_is_answerable_ic
                q.is_answerable_zs = q_is_answerable_zs
            q.filtered = True
            updated_questions.append(q)

    await asyncio.gather(
        *[
            check_questions_for_paragraph(paragraph_id, qs)
            for paragraph_id, qs in questions_by_paragraph.items()
        ]
    )
    return updated_questions


###################################################################################################
#                                       Question Filtering                                        #
###################################################################################################
def is_answerable(question, fact=""):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False
    time.sleep(randwait(WAIT))
    if not fact:
        prompt = f"Is the following question: \n\n {question} \n\n answerable without additional context? \n\n Reply 'YES' and 'NO' only."
    else:
        prompt = f"Is the following question: \n\n {question} \n\n answerable using *only* the following fact? \n\n Fact: {fact} \n\n Reply 'YES' and 'NO' only."

    output = llm_safe_request(
        prompt,
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )
    answer = output["choices"][0]["message"]["content"].strip()
    if answer.startswith(("NO", "no", "No")):
        return False
    elif answer.startswith(("YES", "Yes", "yes")):
        return True
    logging.info("Question Malformed: ", answer)
    return False


async def is_answerable_guided_choice(question, fact=""):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False
    time.sleep(randwait(WAIT))
    if not fact:
        prompt = f"Is the following question: \n\n {question} \n\n a valid question without additional context? \n\n Reply 'YES' and 'NO' only."
    else:
        prompt = f"Is the following question: \n\n {question} \n\n answerable using only the following fact? \n\n Fact: {fact} \n\n Reply 'YES' and 'NO' only."

    output = await llm_safe_request_async(
        prompt,
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
        guided_choice=["YES", "NO"],
    )

    answer = output["choices"][0]["message"]["content"].strip()
    if answer == "NO":
        return False
    elif answer == "YES":
        return True
    logging.info("Question Malformed: ", answer)
    raise Exception("Question Malformed: ", answer)
