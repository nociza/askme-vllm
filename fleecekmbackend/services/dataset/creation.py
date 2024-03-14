import re
import requests
import os
import time
import random
import pandas as pd
import logging
import tqdm

from typing import List, Dict
from fleecekmbackend.core.utils.llm import llm_safe_request, randwait

WAIT = 0.5
MODEL = "togethercomputer/llama-2-70b-chat"
STOP = ["[/INST]", "</s>"]
PROMPT_PREFIX, PROMPT_SUFFIX = ["[INST]", "[/INST]"]

df = pd.read_csv("wiki_text_cleaned.csv")


def is_answerable(question):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False

    time.sleep(randwait(WAIT))
    output = llm_safe_request(
        f"Is the following a well-formed question? reply 'YES' and 'NO' only: \n\n {question}",
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


def regenerate_questions(
    paragraph,
    existing_questions,
    prefix="",  # providing context for fair zeroshot
):
    existing = ""
    for i, q in enumerate(existing_questions):
        existing += f"{i+1}. {q} \n"

    time.sleep(randwait(WAIT))
    output = llm_safe_request(
        f"{PROMPT_PREFIX} Generate a short answer (DO NOT INCLUDE CHOICES) question about the facts mentioned in the following paragraph: {paragraph}\n\n The question should be self-contained; meaning you should not use references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead.\n {PROMPT_SUFFIX} {existing}",
        MODEL,
        STOP,
    )
    return (
        prefix + output["output"]["choices"][0]["text"].strip().split("\n")[0].strip()
    )


def generate_questions(
    paragraph,
    k=3,
    prefix="",
):
    time.sleep(randwait(WAIT))

    prefix = f"In the context of {prefix}, "  # providing context for fair zeroshot

    output = llm_safe_request(
        f"Generate {k} short answer (DO NOT INCLUDE CHOICES) questions about the facts mentioned in the following paragraph in an ordered list separated by linebreak '\n': {paragraph}\n\n",
        MODEL,
        STOP,
        prompt_prefix=PROMPT_PREFIX,
        prompt_suffix=PROMPT_SUFFIX,
    )
    # Extract and store the generated question
    result_raw = output["output"]["choices"][0]["text"]
    result = [
        prefix + x[2:].strip()
        for x in result_raw.split("\n")
        if re.match(r"^[0-9]\.", x)
    ]

    while (len(result) != k or "" in result) and max_attempts > 0:
        time.sleep(randwait(WAIT))
        fixed = llm_safe_request(
            f"Generate {k} short answer (DO NOT INCLUDE CHOICES) questions about the facts mentioned in the following paragraph in an ordered list separated by linebreak '\n': {paragraph}\n\n",
            MODEL,
            STOP,
            prompt_prefix=PROMPT_PREFIX,
            prompt_suffix=PROMPT_SUFFIX,
        )["output"]["choices"][0]["text"]
        max_attempts -= 1
        result = [
            prefix + x[2:].strip()
            for x in fixed.split("\n")
            if re.match(r"^[0-9]\.", x)
        ]

    if max_attempts <= 0:
        raise Exception(
            f"Cannot get {k} questions to the correct format after {max_attempts}"
        )

    good_questions = []
    for q in result:
        if is_answerable(q):
            good_questions.append(q)

    while len(good_questions) < k:
        # print("Good: ", good_questions)
        new_questions = regenerate_questions(paragraph, good_questions, prefix)
        # print("New: ", new_questions)
        for q in [
            prefix + x[2:].strip()
            for x in new_questions.split("\n")
            if re.match(r"^[0-9]\.", x)
        ]:
            if len(good_questions) == k:
                return good_questions
            elif is_answerable(q):
                good_questions.append(q)

    return good_questions


def get_answer(question, reference=None, temperature=0.8, max_token=256):
    time.sleep(randwait(WAIT))
    # retreival
    if reference:
        output = llm_safe_request(
            f"Using this fact: {reference} \n\n Answer the following question in a succinct manner: {question}\n",
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


def generate_dataset(
    dataframe, num_questions=3, csv_file_path="./dataset.csv", return_df=True
):
    df_cols = [
        "index",
        "paragraph",
        "question",
        "ans_zs",
        "ans_rag",
        "rating_zs_score",
        "rating_zs_rationale",
        "rating_rag_score",
        "rating_rag_rationale",
    ]

    df = pd.DataFrame(columns=df_cols)
    if not os.path.isfile(csv_file_path):
        df.to_csv(csv_file_path, mode="w", header=True, index=False)

    def process_row(i, row):
        time.sleep(random.random())
        if row["subsubsection_name"] and row["subsection_name"]:
            fact = f"In an article about {row['page_name']}, section {row['section_name']}, subsection {row['subsection_name']}, paragraph {row['subsubsection_name']} mentioned: {row['text']}"
        elif row["subsection_name"]:
            fact = f"In an article about {row['page_name']}, section {row['section_name']}, subsection {row['subsection_name']} mentioned: {row['text']}"
        else:
            fact = f"In an article about {row['page_name']}, section {row['section_name']} mentioned: {row['text']}"

        currIndices = [f"{i+1}.{x+1}" for x in range(num_questions)]
        currParagraphs = [fact for i in range(num_questions)]
        curr_questions = generate_questions(fact, num_questions, row["page_name"])

        curr_answers_zs = []
        curr_answers_rag = []
        for q in curr_questions:
            curr_answers_zs.append(get_answer(q))
            curr_answers_rag.append(get_answer(q, fact))

        curr_ratings_zs_score = []
        curr_ratings_rag_score = []
        curr_ratings_zs_rationale = []
        curr_ratings_rag_rationale = []

        for i in range(num_questions):
            zs_score, zs_rationale = rate_answer(
                curr_questions[i], curr_answers_zs[i], fact
            )
            curr_ratings_zs_score.append(zs_score)
            curr_ratings_zs_rationale.append(zs_rationale)
            rag_score, rag_rationale = rate_answer(
                curr_questions[i], curr_answers_rag[i], fact
            )
            curr_ratings_rag_score.append(rag_score)
            curr_ratings_rag_rationale.append(rag_rationale)

        temp = {
            "index": currIndices,
            "paragraph": currParagraphs,
            "question": curr_questions,
            "ans_zs": curr_answers_zs,
            "ans_rag": curr_answers_rag,
            "rating_zs_score": curr_ratings_zs_score,
            "rating_zs_rationale": curr_ratings_zs_rationale,
            "rating_rag_score": curr_ratings_rag_score,
            "rating_rag_rationale": curr_ratings_rag_rationale,
        }
        return temp

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
