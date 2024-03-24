import re
import time
import logging

from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple

from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.models import Paragraph, Author, Question, Answer, Rating
from fleecekmbackend.db.helpers import create_author_if_not_exists
from fleecekmbackend.core.utils.llm import llm_safe_request, randwait, generate_prompts_from_template

WAIT = 0.5
MODEL = "togethercomputer/llama-2-70b-chat"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = ["[INST]", "[/INST]"]

NUMQUESTIONS = 3
MAX_ATTEMPTS = 5

############################ Main Function ###############################

async def process_paragraphs(db: AsyncSession, paragraphs: List[Paragraph]) -> Tuple[List[Question], List[Answer], List[Rating]]:
    generated_questions = []
    generated_answers = []
    generated_ratings = []
    try:
        for paragraph in paragraphs:
            try:
                async with db.begin():
                    # Generate questions
                    questions = await generate_questions(db, paragraph)
                    generated_questions.extend(questions)

                    for question in questions:
                        try:
                            async with db.begin_nested():
                                # Generate answers
                                for setting in ["zs", "ic"]:
                                    answer = await generate_answer(db, question, setting)
                                    generated_answers.append(answer)

                                    # Generate answer ratings
                                    rating = await generate_answer_rating(db, question, answer)
                                    generated_ratings.append(rating)

                        except Exception as e:
                            logging.error(f"Error processing question: {question.text}")
                            logging.error(str(e))
                            raise

            except Exception as e:
                logging.error(f"Error processing paragraph: {paragraph.id}")
                logging.error(str(e))
                raise

    except Exception as e:
        await db.rollback()
        raise

    return generated_questions, generated_answers, generated_ratings

############################ Generation Functions ############################
# Generate questions for a paragraph
async def generate_questions(
    db: AsyncSession,
    paragraph: Paragraph,
    k: int = NUMQUESTIONS,
    max_attempts: int = MAX_ATTEMPTS,
):
    # process prompt template
    time.sleep(randwait(WAIT))
    prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} additional short answer (DO NOT INCLUDE CHOICES) questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead.\n\nExisting questions:\n{EXISTING_QUESTIONS}\n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
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
    author = create_author_if_not_exists(db, template, MODEL)
    
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
        time.sleep(randwait(WAIT))
        output = llm_safe_request(prompt, MODEL, STOP)
        new_questions = [
            x[2:].strip()
            for x in output["output"]["choices"][0]["text"].strip().split("\n")
            if re.match(r"^[0-9]\.", x)
        ]
        return existing_questions + new_questions

    # main loop
    good_questions = []
    attempts = 0
    while len(good_questions) < k and attempts < max_attempts:
        attempts += 1
        questions = await generate_or_regenerate_questions(good_questions)
        good_questions = [q for q in questions if is_answerable(q)]
    if len(good_questions) < k:
        raise Exception(
            f"Cannot get {k} questions to the correct format after {max_attempts} attempts"
        )
    
    # Add questions to the database
    for q in good_questions:
        question = Question(
            paragraph_id=paragraph.id,
            scope="single-paragraph",
            text=q,
            context=context,
            author_id=author.id,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            upvote=0,
            downvote=0,
        )
        db.add(question)
        await db.commit()
    return good_questions

async def generate_answer(
    db: AsyncSession,
    question: Question,
    context: str = None,
    max_attempts: int = MAX_ATTEMPTS,
):
    # process prompt template
    time.sleep(randwait(WAIT))
    prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in a succinct manner: {QUESTION}\n{PROMPT_SUFFIX}"

    paragraph = await db.get(Paragraph, question.paragraph_id)
    _, fact = generate_fact_with_context(paragraph)

    if context:
        setting = "ic"
        context_prompt = f"Using this fact: {fact} \n\n "
    else:
        setting = "zs"
        context_prompt = ""

    prompt, template = generate_prompts_from_template(
        prompt_template,
        {
            "CONTEXT_PROMPT": context_prompt,
            "QUESTION": question.text,
            "PROMPT_PREFIX": PROMPT_PREFIX,
            "PROMPT_SUFFIX": PROMPT_SUFFIX,
        },
    )
    author = create_author_if_not_exists(db, template, MODEL)

    # main loop
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        time.sleep(randwait(WAIT))
        output = llm_safe_request(prompt, MODEL, STOP)
        answer_text = output["output"]["choices"][0]["text"].strip()

        if answer_text:
            answer = Answer(
                question_id=question.id,
                author_id=author.id,
                setting=setting,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                text=answer_text,
            )
            db.add(answer)
            await db.commit()
            return answer

    raise Exception(
        f"Cannot generate a valid answer after {max_attempts} attempts. \n Question: {question.text}"
    )

async def generate_answer_rating(
    db: AsyncSession,
    question: Question,
    answer: Answer,
    max_attempts: int = MAX_ATTEMPTS,
):
    # process prompt template
    time.sleep(randwait(WAIT))
    prompt_template = "{PROMPT_PREFIX}Based on this fact: \n\n `{REFERENCE}` \n\n Rate the following answer to the question - Question: `{QUESTION}` \n\n Answer: `{ANSWER}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n{PROMPT_SUFFIX}"

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

    author = create_author_if_not_exists(db, template, MODEL)

    # main loop
    attempts = 0
    while attempts < max_attempts:
        attempts += 1
        time.sleep(randwait(WAIT))
        output = llm_safe_request(prompt, MODEL, STOP)
        rating_raw = output["output"]["choices"][0]["text"]

        if re.search(r"Rationale:", rating_raw, re.I) and re.search(r"[0-5]", rating_raw):
            score = int(re.search(r"[0-5]", rating_raw).group())
            rationale = "".join(rating_raw.split("Rationale:", re.I)[1:]).strip()

            rating = Rating(
                text=rationale,
                value=score,
                answer_id=answer.id,
                author_id=author.id,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            db.add(rating)
            await db.commit()

            return rating

    raise Exception(
        f"Cannot rate answers to the correct format after {max_attempts} attempts."
    )

############################ Helper Functions ############################
def generate_fact_with_context(paragraph: Paragraph):
    if paragraph.subsubsection_name and paragraph.subsection_name:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}, subsection {paragraph.subsection_name}, paragraph {paragraph.subsubsection_name}"
    elif paragraph.subsection_name:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}, subsection {paragraph.subsection_name}"
    else:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}"
    return context, f"{context} mentioned: {paragraph.text}" 

def is_answerable(question):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False
    time.sleep(randwait(WAIT))
    output = llm_safe_request(
        f"Is the following a well-formed question? Reply 'YES' and 'NO' only: \n\n {question}",
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )
    answer = output["output"]["choices"][0]["text"].strip()
    if answer.strip().startswith(("NO", "no", "No")):
        return False
    elif answer.strip().startswith(("YES", "Yes", "yes")):
        return True
    logging.info("Question Malformed: ", answer)
    return False