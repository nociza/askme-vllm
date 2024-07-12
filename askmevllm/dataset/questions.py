from datetime import datetime
import logging
import re
import json
import traceback
from typing import List, Optional

from vllm import LLM, SamplingParams
from outlines.serve.vllm import JSONLogitsProcessor
from pydantic import BaseModel
from askmevllm.models import Question, Paragraph, dataset
from askmevllm.dataset.common import generate_fact_with_context
from askmevllm.helpers import create_author_if_not_exists
from askmevllm.config import NUMQUESTIONS, TEMPERATURE


def generate_questions_single_turn(
    paragraphs: List[Paragraph], llm, k: int = NUMQUESTIONS
) -> List[List[Question]]:
    try:
        prompts = []
        for paragraph in paragraphs:
            prompt_template = "{PROMPT_PREFIX}Generate {NUM_QUESTIONS} short answer questions about the facts mentioned in the following paragraph. The questions should be self-contained; meaning you avoid using references such as 'it', 'the game', 'the person', etc., but should directly include the name of the referenced item instead. Remember to include relevant context in the question. Return a ordered list. \n\nParagraph: {PARAGRAPH}\n{PROMPT_SUFFIX}"
            context, fact = generate_fact_with_context(paragraph)
            prompt = prompt_template.format(
                PARAGRAPH=fact, PROMPT_PREFIX="", PROMPT_SUFFIX="", NUM_QUESTIONS=k
            )
            prompts.append(prompt)

        author_id = create_author_if_not_exists(prompts[0], "llama3-70B-instruct")

        logging.debug("Generating questions for paragraphs")

        sampling_params = SamplingParams(max_tokens=500, temperature=TEMPERATURE)
        outputs = llm.generate(prompts, sampling_params)
        all_question_objects = []

        for paragraph, output in zip(paragraphs, outputs):
            generated_text = output.outputs[0].text.strip()
            logging.debug(f"Generated questions: {generated_text}")

            new_questions = [
                re.sub(r"^\d\.", "", x).strip()
                for x in generated_text.split("\n")
                if re.match(r"^[0-9]\.", x)
            ]

            print(new_questions)

            question_objects = [
                Question(
                    id=len(dataset.questions) + 1,
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
            all_question_objects.extend(question_objects)

        return all_question_objects

    except Exception as e:
        logging.error(str(e))
        logging.error(traceback.format_exc())
        raise Exception("Error generating questions for paragraphs") from e


def filter_questions(questions: List[Question], llm) -> List[Question]:
    updated_questions = []

    questions_by_paragraph = {}
    for q in questions:
        if q.paragraph_id not in questions_by_paragraph:
            questions_by_paragraph[q.paragraph_id] = []
        questions_by_paragraph[q.paragraph_id].append(q)

    for paragraph_id, qs in questions_by_paragraph.items():
        paragraph = next(p for p in dataset.paragraphs if p.id == paragraph_id)
        context, fact = generate_fact_with_context(paragraph)

        question_texts = [q.text for q in qs]

        # Process all questions for this paragraph at once
        ic_results = is_answerable_guided_choice(question_texts, llm, [fact] * len(qs))
        zs_results = is_answerable_guided_choice(question_texts, llm)

        for q, ic_result, zs_result in zip(qs, ic_results, zs_results):
            logging.debug(f"Checking if answerable: {q.text}")
            logging.debug(
                f"Answerable in IC: {ic_result}, Answerable in ZS: {zs_result}"
            )
            if not ic_result or not zs_result:
                q.rejected = True
            q.is_answerable_ic = ic_result
            q.is_answerable_zs = zs_result
            q.filtered = True
            updated_questions.append(q)

    return updated_questions


def is_answerable(question, fact, llm):
    if not question.strip():
        logging.debug("No question seen in is_answerable: ", question.strip())
        return False

    if not fact:
        prompt = f"Is the following question: \n\n {question} \n\n a valid question without additional context? \n\n Reply 'YES' and 'NO' only."
    else:
        prompt = f"Is the following question: \n\n {question} \n\n answerable using only the following fact? \n\n Fact: {fact} \n\n Reply 'YES' and 'NO' only."

    sampling_params = SamplingParams(max_tokens=10, temperature=0.0)
    output = llm.generate([prompt], sampling_params)
    answer = output[0].text.strip()
    if answer == "NO":
        return False
    elif answer == "YES":
        return True
    logging.info("Question Malformed: ", answer)
    raise Exception("Question Malformed: ", answer)


class YesNoOutput(BaseModel):
    text: str


def is_answerable_guided_choice(
    questions: List[str], llm: LLM, facts: Optional[List[str]] = None
) -> List[bool]:
    if not questions:
        logging.debug("No questions seen in is_answerable")
        return []

    if facts is None:
        facts = [""] * len(questions)
    elif len(facts) != len(questions):
        raise ValueError("The number of facts must match the number of questions")

    prompts = []
    for question, fact in zip(questions, facts):
        if not question.strip():
            logging.debug(f"Empty question seen in is_answerable: {question.strip()}")
            continue

        if not fact:
            prompt = f"Is the following question: \n\n {question} \n\n a valid question without additional context? \n\n Reply 'Y' and 'N' only."
        else:
            prompt = f"Is the following question: \n\n {question} \n\n answerable using only the following fact? \n\n Fact: {fact} \n\n Reply 'Y' and 'N' only."
        prompts.append(prompt)

    logits_processor = JSONLogitsProcessor(schema=YesNoOutput, llm=llm.llm_engine)
    logits_processor.fsm.vocabulary = ["Y", "N"]
    sampling_params = SamplingParams(
        max_tokens=10, temperature=TEMPERATURE, logits_processors=[logits_processor]
    )

    outputs = llm.generate(prompts, sampling_params)

    results = []
    for output in outputs:
        try:
            answer = json.loads(output.outputs[0].text.strip())
            if answer["text"] == "N":
                results.append(False)
            elif answer["text"] == "Y":
                results.append(True)
            else:
                logging.info(f"Question Malformed: {answer}")
                results.append(False)
        except Exception as e:
            logging.error(f"Error processing answer: {e}")
            results.append(False)

    return results
