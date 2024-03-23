import re
import requests
import os
import time
import random
import pandas as pd
import logging
import tqdm
import hashlib

from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

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

# Generate an answer to a question
def generate_answer(db: AsyncSession, question: Question, context: str = None):
    time.sleep(randwait(WAIT))
    # in context
    if context:
        
        output = llm_safe_request(
            f"Using this fact: {context} \n\n Answer the following question in a succinct manner: {question}\n",
            MODEL,
            STOP,
            prompt_prefix=PROMPT_PREFIX,
            prompt_suffix=PROMPT_SUFFIX,
        )
        return output["output"]["choices"][0]["text"]

    # zeroshot
    output = llm_safe_request(
        f"Answer the following question in a succinct manner: {question}\n",
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )
    return output["output"]["choices"][0]["text"]

def rate_answer(
    question,
    answer,
    reference,
):
    time.sleep(randwait(WAIT))
    output = llm_safe_request(
        f"Based on this fact: \n\n `{reference}` \n\n Rate the following answer to the question - Question: `{question}` \n\n Answer: `{answer}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n",
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )

    rating_raw = output["output"]["choices"][0]["text"]

    while (
        not re.search(r"Rationale:", rating_raw, re.I)
        or not re.search(r"[0-5]", rating_raw)
    ) and max_attempts > 0:
        time.sleep(randwait(WAIT))
        rating_raw = llm_safe_request(
            f"Based on this fact: \n\n `{reference}` \n\n Rate the following answer to the question - Question: `{question}` \n\n Answer: `{answer}`; give a number from 0-5 where 0 is 'No answer or completely irrelevant', 1 is 'Significantly incorrect or incomplete', 2 is 'Partially correct; major inaccuracies or omissions', 3 is 'Correct but lacks depth; minimal detail', 4 is 'Mostly correct; minor errors, includes relevant details', 5 is 'Fully accurate and detailed; clear and comprehensive'. Your answer should follow the form `Answer:<number> \n Rationale:<justify your judgment in a paragraph>`. \n",
            MODEL,
            STOP,
            prompt_prefix=PROMPT_PREFIX,
            prompt_suffix=PROMPT_SUFFIX,
        )["output"]["choices"][0]["text"]
        max_attempts -= 1

    if not re.search(r"Rationale:", rating_raw, re.I) or not re.search(
        r"[0-5]", rating_raw
    ):
        raise Exception(
            f"Cannot rate answers to the correct format after max_attempts. \n Question: ",
            question,
            "\n Answer: ",
            answer,
            "\n Reference: ",
            reference,
            "\n Rating: ",
            rating_raw,
        )

    score = int(re.search(r"[0-5]", rating_raw).group())
    rationale = "".join(rating_raw.split("Rationale:", re.I)[1:]).strip()

    return score, rationale

def generate_fact_with_context(paragraph: Paragraph):
    if paragraph.subsubsection_name and paragraph.subsection_name:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}, subsection {paragraph.subsection_name}, paragraph {paragraph.subsubsection_name}"
    elif paragraph.subsection_name:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}, subsection {paragraph.subsection_name}"
    else:
        context = f"In an article about {paragraph.page_name}, section {paragraph.section_name}"
    return context, f"{context} mentioned: {paragraph.text}" 

async def process_samples(db: AsyncSession, paragraphs: list[Paragraph]):
    for paragraph in paragraphs:

        curr_questions = generate_questions(db, paragraph)

        for i, question_text in enumerate(curr_questions):
            author = Author(model=MODEL)
            db.add(author)
            await db.commit()

            question = Question(
                paragraph_id=paragraph.id,
                scope="single-paragraph",
                text=question_text,
                author_id=author.id,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                upvote=0,
                downvote=0,
            )
            db.add(question)
            await db.commit()

            for setting in ["zs", "ic"]:
                answer_text = generate_answer(question_text, fact if setting == "ic" else None)
                answer_author = Author(model=MODEL)
                db.add(answer_author)
                await db.commit()

                answer = Answer(
                    question_id=question.id,
                    author_id=answer_author.id,
                    setting=setting,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    text=answer_text,
                )
                db.add(answer)
                await db.commit()

                score, rationale = rate_answer(question_text, answer_text, fact)
                rating_author = Author(model=MODEL)
                db.add(rating_author)
                await db.commit()

                rating = Rating(
                    text=rationale,
                    value=score,
                    answer_id=answer.id,
                    author_id=rating_author.id,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
                db.add(rating)
                await db.commit()


############################### Under Development ################################
def rate_answer_score(
    question,
    answer,
    reference,
):
    time.sleep(randwait(WAIT))
    output = llm_safe_request(
        f"Based on this fact: {reference}, \n rate the following answer to the question '{question}': {answer}; give a number from 0-5 where 0 is completely incorrect and 5 is completely correct. Your answer should only include a number between 0 and 5 and nothing else.",
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )
    rating_raw = output["output"]["choices"][0]["text"]



#################################### Legacy ######################################
def process_row(row, i=-1, num_questions=NUMQUESTIONS):
    time.sleep(random.random())
    if row["subsubsection_name"] and row["subsection_name"]:
        fact = f"In an article about {row['page_name']}, section {row['section_name']}, subsection {row['subsection_name']}, paragraph {row['subsubsection_name']} mentioned: {row['text']}"
    elif row["subsection_name"]:
        fact = f"In an article about {row['page_name']}, section {row['section_name']}, subsection {row['subsection_name']} mentioned: {row['text']}"
    else:
        fact = f"In an article about {row['page_name']}, section {row['section_name']} mentioned: {row['text']}"

    currIndices = [f"{i+1}.{x+1}" for x in range(num_questions)]
    currParagraphs = [fact for i in range(num_questions)]

    print(fact, num_questions, row["page_name"])
    curr_questions = generate_questions(fact, num_questions, row["page_name"])

    curr_answers_zs = []
    curr_answers_ic = []
    for q in curr_questions:
        curr_answers_zs.append(generate_answer(q))
        curr_answers_ic.append(generate_answer(q, fact))

    curr_ratings_zs_score = []
    curr_ratings_ic_score = []
    curr_ratings_zs_rationale = []
    curr_ratings_ic_rationale = []

    for i in range(num_questions):
        zs_score, zs_rationale = rate_answer(
            curr_questions[i], curr_answers_zs[i], fact
        )
        curr_ratings_zs_score.append(zs_score)
        curr_ratings_zs_rationale.append(zs_rationale)
        rag_score, rag_rationale = rate_answer(
            curr_questions[i], curr_answers_ic[i], fact
        )
        curr_ratings_ic_score.append(rag_score)
        curr_ratings_ic_rationale.append(rag_rationale)

    curr_hash = []
    for i in range(num_questions):
        data = f"{currParagraphs[i]}{curr_questions[i]}{curr_answers_zs[i]}{curr_answers_ic[i]}".encode('utf-8')
        curr_hash.append(hashlib.sha256(data).hexdigest())

    curr_paragraph_hash = hashlib.sha256(fact.encode('utf-8')).hexdigest()

    temp = {
        "index": currIndices, # only used for whole dataset generation
        "paragraph": currParagraphs,
        "paragraph_hash": curr_paragraph_hash,
        "question": curr_questions,
        "ans_zs": curr_answers_zs,
        "ans_ic": curr_answers_ic,
        "rating_zs_score": curr_ratings_zs_score,
        "rating_zs_rationale": curr_ratings_zs_rationale,
        "rating_ic_score": curr_ratings_ic_score,
        "rating_ic_rationale": curr_ratings_ic_rationale,
        "hash": curr_hash
    }
    return temp

def generate_dataset(
    dataframe, num_questions=3, csv_file_path="./dataset.csv", return_df=True
):
    df_cols = [
        "index",
        "paragraph",
        "question",
        "ans_zs",
        "ans_ic",
        "rating_zs_score",
        "rating_zs_rationale",
        "rating_ic_score",
        "rating_ic_rationale",
        "hash"
    ]

    df = pd.DataFrame(columns=df_cols)
    if not os.path.isfile(csv_file_path):
        df.to_csv(csv_file_path, mode="w", header=True, index=False)
        
    for i, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0]):
        while True:
            try:
                result = process_row(i, row)
                break
            except requests.exceptions.HTTPError:
                print("Encountered a server error, retrying...")
                time.sleep(randwait(WAIT))

        curr_df = pd.DataFrame(result)
        curr_df.to_csv(csv_file_path, mode="a", header=False, index=False)
        if return_df:
            df = pd.concat([df, curr_df], ignore_index=True)
    return df