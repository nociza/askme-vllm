import re
import time
import logging
from datetime import datetime
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple
from fleecekmbackend.db.ctl import async_session
from fleecekmbackend.db.models import Paragraph, Question, Answer, Rating
from fleecekmbackend.db.helpers import create_author_if_not_exists
from fleecekmbackend.core.utils.llm import llm_safe_request, randwait, generate_prompts_from_template

WAIT = 0.1
MODEL = "mistralai/Mixtral-8x7B-Instruct-v0.1"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = ["[INST]", "[/INST]"]

NUMQUESTIONS = 3
MAX_ATTEMPTS = 5

############################ Main Functions ############################
async def process_paragraph(db: AsyncSession, paragraph: Paragraph) -> Tuple[List[Question], List[Answer], List[Rating]]:
    generated_question_ids = []
    generated_answer_ids = []
    generated_rating_ids = []
    try:
        paragraph_id = paragraph.id
        logging.info(f"Processing paragraph: {paragraph_id}")

        question_ids = await generate_questions(db, paragraph)

        logging.info(f"generated_questions: {question_ids}")

        generated_question_ids.extend(question_ids)
        for question_id in question_ids:
            try:
                for setting in ["zs", "ic"]:
                    # Generate answers
                    answer_id = await generate_answer(db, question_id, setting)
                    generated_answer_ids.append(answer_id)
                    logging.info(f"generated_answer_id: {answer_id}")

                    # Generate answer ratings
                    rating_id = await generate_answer_rating(db, question_id, answer_id)
                    generated_rating_ids.append(rating_id)
                    logging.info(f"generated_rating_id: {rating_id}")

            except Exception as e:
                logging.error(f"Error processing question: {question_id.text}")
                logging.error(str(e))
                raise
        
        async with async_session() as session:
            largest_processed = (await session.execute(select(func.max(Paragraph.processed)))).scalar()
            if largest_processed is None:
                raise Exception("largest_processed is None")
            print("largest_processed: ", largest_processed)
            await session.execute(
                update(Paragraph).where(Paragraph.id == paragraph_id).values(processed=largest_processed + 1)
            )
            await session.commit()
            logging.info(f"Processed paragraph: {paragraph_id}")

    except Exception as e:
        await db.rollback()
        logging.error(str(e))
        raise
    return generated_question_ids, generated_answer_ids, generated_rating_ids

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
        logging.error(str(e))
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

        author = await create_author_if_not_exists(template, MODEL)

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
            logging.info(f"Generated questions: {output['choices'][0]['message']['content']}")
            new_questions = [
                x[2:].strip()
                for x in output["choices"][0]['message']['content'].strip().split("\n")
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
                logging.info(f"Answerable in IC: {q_is_answerable_ic}, Answerable in ZS: {q_is_answerable_zs}")
                if q_is_answerable_ic and q_is_answerable_zs:
                    good_questions.append(q)
            logging.info(f"Good Questions {attempts}: {good_questions}")
        if len(good_questions) < k:
            logging.error(f"Failed to get {k} questions after {max_attempts} attempts, current number of questions: {len(good_questions)}")

        logging.info(f"Good Questions: {good_questions}")

        question_objs = []
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
            logging.info(f"Adding question: {question.text}")
            async with async_session() as session:
                session.add(question)
                await session.commit()
                await session.refresh(question, ["id"])
                question_objs.append(question.id)
        return question_objs
    except Exception as e:
        logging.error(str(e))
        raise

async def generate_answer(
    db: AsyncSession,
    question_id: int,
    context: str = None,
    max_attempts: int = MAX_ATTEMPTS,
):
    # process prompt template
    async with async_session() as session:
        question = await session.get(Question, question_id)

        prompt_template = "{PROMPT_PREFIX}{CONTEXT_PROMPT}Answer the following question in a succinct manner: {QUESTION}\n{PROMPT_SUFFIX}"

        paragraph = await session.get(Paragraph, question.paragraph_id)
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
        author = await create_author_if_not_exists(template, MODEL)

        # main loop
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            time.sleep(randwait(WAIT))
            output = llm_safe_request(prompt, MODEL, STOP)
            answer_text = output["choices"][0]['message']['content'].strip()

            if answer_text:
                answer = Answer(
                    question_id=question.id,
                    author_id=author.id,
                    setting=setting,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    text=answer_text,
                )
                logging.info(f"Generated answer: {answer.text}")
                session.add(answer)
                await session.commit()
                await session.refresh(answer, ["id"])
                return answer.id

        raise Exception(
            f"Cannot generate a valid answer after {max_attempts} attempts. \n Question: {question.text}"
        )

async def generate_answer_rating(
    db: AsyncSession,
    question_id: int,
    answer_id: int,
    max_attempts: int = MAX_ATTEMPTS,
):
    try:
        prompt_template = "{PROMPT_PREFIX}Based on this fact: \n\n `{REFERENCE}` \n\n Rate the following answer to the question - Question: `{QUESTION}` \n\n Answer: `{ANSWER}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n{PROMPT_SUFFIX}"

        async with async_session() as session:
            question = await session.get(Question, question_id)
            paragraph = await session.get(Paragraph, question.paragraph_id)
            answer = await session.get(Answer, answer_id)
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

            author = await create_author_if_not_exists(template, MODEL)

            # main loop
            attempts = 0
            while attempts < max_attempts:
                attempts += 1
                time.sleep(randwait(WAIT))
                output = llm_safe_request(prompt, MODEL, STOP)
                rating_raw = output["choices"][0]['message']['content']

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
                    session.add(rating)
                    await session.commit()
                    await session.refresh(rating, ["id"])
                    return rating.id

        raise Exception(
            f"Cannot rate answers to the correct format after {max_attempts} attempts."
        )
    except Exception as e:
        logging.error(f"An error occurred at generate_answer_rating: {e}")

############################ Helper Functions ############################
def generate_fact_with_context(paragraph: Paragraph):
    if paragraph.subsubsection_name and paragraph.subsection_name:
        context = f"In an article about \'{paragraph.page_name}\', section \'{paragraph.section_name}\', subsection \'{paragraph.subsection_name}\', paragraph \'{paragraph.subsubsection_name}\'"
    elif paragraph.subsection_name:
        context = f"In an article about \'{paragraph.page_name}\', section \'{paragraph.section_name}\', subsection \'{paragraph.subsection_name}\'"
    else:
        context = f"In an article about \'{paragraph.page_name}\', section \'{paragraph.section_name}\'"
    return context, f"{context} mentioned: \n {paragraph.text}" 

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
